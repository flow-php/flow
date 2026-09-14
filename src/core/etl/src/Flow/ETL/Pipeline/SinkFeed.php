<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SideRootFailure;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Loader\Closure;
use Flow\ETL\Loader\Discardable;
use Flow\ETL\Rows;
use Throwable;

use function array_values;

/**
 * @internal the planner's step that feeds one side pipeline: a non-bare sink root, or a node several sinks share
 */
final class SinkFeed implements Closure, Discardable, Loader
{
    /**
     * @var non-empty-list<Loader>
     */
    private readonly array $consumers;

    /**
     * @param Loader ...$consumers the side pipeline's last steps, which a completed run's discard() is forwarded to
     *
     * @throws InvalidArgumentException when no consumer is given
     */
    public function __construct(
        private readonly FeedExtractor $feed,
        private readonly SideRun $run,
        private readonly SideOffers $offers,
        Loader ...$consumers,
    ) {
        $consumers = array_values($consumers);

        if ($consumers === []) {
            throw new InvalidArgumentException('At least one consumer must be provided');
        }

        $this->consumers = $consumers;
    }

    public function load(Rows $rows, FlowContext $context): void
    {
        $this->feed->feed($rows);
        $this->offers->forget();

        // offered INSIDE the side pipeline -> wrapped, so the outer Segment does not offer it twice. An ENDING
        // failure (a limit inside the side pipeline completed it mid-load) was offered to nobody -> raw, so the outer
        // Segment offers it exactly once, against this step.
        try {
            $this->run->advance();
        } catch (Throwable $failure) {
            throw $this->offers->offered($failure) ? new SideRootFailure($failure) : $failure;
        }
    }

    public function closure(FlowContext $context): void
    {
        $this->feed->finish();

        // raw: endLoaders() never offers, so the user's own exception class must surface
        try {
            $this->run->advance();
        } finally {
            $this->run->drop();
        }
    }

    public function discard(FlowContext $context): void
    {
        if ($this->run->terminated()) {
            // the side Segment's finally already ended the consumers - closure() on completion, discard() on
            // failure - so forward only after a completed run, and never twice
            if ($this->run->completed()) {
                foreach ($this->consumers as $consumer) {
                    if ($consumer instanceof Discardable) {
                        $consumer->discard($context);
                    }
                }
            }

            return;
        }

        // start-or-resume and unwind: the side Segment ends its consumers with completed: false
        try {
            $this->run->advance();
        } finally {
            $this->run->drop();
        }
    }

    /**
     * After a rollback: the next batch runs a fresh fiber over the same side pipeline.
     */
    public function restart(): void
    {
        $this->run->drop();
    }
}

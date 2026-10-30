<?php

declare(strict_types=1);

namespace Flow\ETL\Runtime\Executor;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\IO\Extractor\FeedExtractor;
use Flow\ETL\IO\Loader;
use Flow\ETL\IO\Loader\Discardable;
use Flow\ETL\Runtime\ErrorHandler\LoadingError;
use Flow\ETL\Runtime\ErrorHandler\TransformationError;
use Flow\ETL\Values\Rows;
use Throwable;

use function array_values;

/**
 * The planner's step that feeds one branch pipeline: a write with steps of its own, or a node several writes share.
 */
final class BranchLoader implements NestedLoader
{
    /**
     * @var non-empty-list<Loader|NestedLoader>
     */
    private readonly array $writes;

    /**
     * @param Loader|NestedLoader ...$writes the branch pipeline's last steps, which a completed run's discard() is
     *                                       forwarded to
     *
     * @throws InvalidArgumentException when no write is given
     */
    public function __construct(
        private readonly FeedExtractor $feed,
        private readonly BranchFiber $run,
        private readonly OfferedFailure $offers,
        Loader|NestedLoader ...$writes,
    ) {
        $writes = array_values($writes);

        if ($writes === []) {
            throw new InvalidArgumentException('At least one write must be provided');
        }

        $this->writes = $writes;
    }

    public function load(Rows $rows): LoadingError|TransformationError|null
    {
        $this->feed->feed($rows);
        $this->offers->forget();

        try {
            $this->run->advance();
        } catch (Throwable $failure) {
            // the failed fiber is this branch's own, so the next batch runs a fresh one over the same pipeline
            $this->run->drop();

            return $this->offers->undecided($failure) ?? throw $failure;
        }

        return null;
    }

    public function closure(): void
    {
        $this->feed->finish();

        // raw: endLoaders() never offers, so the user's own exception class must surface
        try {
            $this->run->advance();
        } finally {
            $this->run->drop();
        }
    }

    public function discard(): void
    {
        if ($this->run->terminated()) {
            // the side PipelineSteps' finally already ended the writes - closure() on completion, discard() on
            // failure - so forward only after a completed run, and never twice
            if ($this->run->completed()) {
                foreach ($this->writes as $write) {
                    if ($write instanceof Discardable) {
                        $write->discard();
                    }
                }
            }

            return;
        }

        // start-or-resume and unwind: the side PipelineSteps ends its writes with completed: false
        try {
            $this->run->advance();
        } finally {
            $this->run->drop();
        }
    }
}

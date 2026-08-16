<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Fiber;
use FiberError;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Transformation;

use function Flow\ETL\DSL\df;

/**
 * One nested pipeline per outer run: built once, fed batch by batch, the sink called from inside the Fiber with
 * the outer FlowContext. Owns no failure policy - a throwable from feed()/drain() propagates and the owner
 * discards the whole drive; a drive terminated by a nested limit ignores later feeds and drains as a no-op.
 *
 * @internal
 */
final readonly class TransformationDrive
{
    private Fiber $fiber;

    private FeedExtractor $source;

    public function __construct(
        Transformation $transformation,
        private Loader $sink,
        private FlowContext $context,
    ) {
        $this->source = new FeedExtractor();

        try {
            $frame = $transformation->transform(df($context->config)->from($this->source));

            // @mago-ignore analysis:avoid-catching-error
            // Nothing is hidden - the FiberError is rethrown as the previous exception. It only ever means the
            // Transformation triggered the frame, which no message from the engine explains.
        } catch (FiberError $error) {
            throw new InvalidLogicException(
                'A Transformation given to to_transformation() or to_branch()->withTransformation() must only '
                . 'build the DataFrame, not trigger it - count(), fetch(), schema() and the other trigger '
                . 'methods read from a source that only exists while the nested pipeline runs.',
                previous: $error,
            );
        }

        $this->fiber = new Fiber(function () use ($frame): void {
            foreach ($frame->get() as $transformedRows) {
                $this->sink->load($transformedRows, $this->context);
            }
        });
    }

    public function drain(): void
    {
        if ($this->fiber->isSuspended()) {
            $this->source->finish();
            $this->fiber->resume();
        }
    }

    public function drivenBy(FlowContext $context): bool
    {
        return $this->context === $context;
    }

    public function feed(Rows $rows): void
    {
        if ($this->fiber->isTerminated()) {
            return;
        }

        $this->source->feed($rows);

        $this->fiber->isStarted() ? $this->fiber->resume() : $this->fiber->start();
    }
}

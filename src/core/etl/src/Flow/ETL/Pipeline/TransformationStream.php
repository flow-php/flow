<?php

declare(strict_types=1);

namespace Flow\ETL\Pipeline;

use Fiber;
use FiberError;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Extractor\FeedExtractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Transformation;

use function Flow\ETL\DSL\df;

/**
 * @internal
 */
final readonly class TransformationStream
{
    private Fiber $fiber;

    private FeedExtractor $source;

    public function __construct(
        Transformation $transformation,
        Schema $schema,
        private Loader $sink,
        private FlowContext $context,
    ) {
        $this->source = new FeedExtractor($schema);

        try {
            $frame = $transformation->transform(df($context->config)->from($this->source));

            // @mago-ignore analysis:avoid-catching-error
        } catch (FiberError $error) {
            throw new InvalidLogicException(
                'A Transformation given to to_transformation() or to_branch()->withTransformation() must only '
                . 'build the DataFrame, not trigger it - count(), fetch() and the other trigger methods read '
                . 'from a source that only exists while the nested pipeline runs.',
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

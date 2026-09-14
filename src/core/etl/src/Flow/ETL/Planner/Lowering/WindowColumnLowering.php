<?php

declare(strict_types=1);

namespace Flow\ETL\Planner\Lowering;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Plan\Node;
use Flow\ETL\Plan\Node\WindowColumn;
use Flow\ETL\Planner\Lowering;
use Flow\ETL\Processor;
use Flow\ETL\Processor\CollectingProcessor;
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Repartition\RepartitionSteps;
use Flow\ETL\Transformer;

/**
 * @implements Lowering<WindowColumn>
 */
final readonly class WindowColumnLowering implements Lowering
{
    /**
     * @return class-string<WindowColumn>
     */
    public function handles(): string
    {
        return WindowColumn::class;
    }

    /**
     * @param WindowColumn $node
     *
     * @return list<Loader|Processor|Transformer>
     */
    public function steps(Node $node, FlowContext $context, array $frames): array
    {
        return (
            $node->function->window()->partitions()->count()
                ? [
                    ...RepartitionSteps::of($node->function->window()->partitions(), $context->config),
                    new WindowProcessor($node->entry, $node->function),
                ]
                : [new CollectingProcessor(), new WindowProcessor($node->entry, $node->function)]
        );
    }
}

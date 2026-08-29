<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\FlowContext;
use Flow\ETL\GroupBy;
use Flow\ETL\GroupBy\GroupBySteps;
use Flow\ETL\Processor;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;
use Generator;

final class GroupByContext
{
    /**
     * Runs input batches through the GroupBySteps chain the way Pipeline does.
     *
     * @return list<Rows>
     */
    public static function aggregate(GroupBy $groupBy, FlowContext $context, Rows ...$batches): array
    {
        $output = self::batches(...$batches);

        foreach (GroupBySteps::of($groupBy, $context->config) as $step) {
            $output = $step instanceof Processor
                ? $step->process($output, $context)
                : self::transformed($step, $output, $context);
        }

        return iterator_to_array($output, preserve_keys: false);
    }

    /**
     * All aggregated batches merged into one Rows, for equality assertions.
     */
    public static function aggregated(GroupBy $groupBy, FlowContext $context, Rows ...$batches): Rows
    {
        $result = new Rows();

        foreach (self::aggregate($groupBy, $context, ...$batches) as $batch) {
            $result = $result->merge($batch);
        }

        return $result;
    }

    /**
     * @return Generator<Rows>
     */
    public static function batches(Rows ...$batches): Generator
    {
        yield from $batches;
    }

    /**
     * @param Generator<Rows> $upstream
     *
     * @return Generator<Rows>
     */
    public static function transformed(Transformer $step, Generator $upstream, FlowContext $context): Generator
    {
        foreach ($upstream as $batch) {
            yield $step->transform($batch, $context);
        }
    }
}

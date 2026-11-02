<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Context;

use Flow\ETL\Operators\GroupBy;
use Flow\ETL\Operators\Processor;
use Flow\ETL\Operators\Processor\GroupByAggregationProcessor;
use Flow\ETL\Operators\Transformer;
use Flow\ETL\Planning\Logical\Node\Aggregate;
use Flow\ETL\Runtime\FlowContext;
use Flow\ETL\Tests\Mother\NodeMother;
use Flow\ETL\Tests\Mother\NodeTranslatorMother;
use Flow\ETL\Values\Column\PhpBackend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Generator;

use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\Types\DSL\type_instance_of;

final class GroupByContext
{
    /**
     * Runs input batches through the aggregate's steps the way Pipeline does.
     *
     * @return list<Rows>
     */
    public static function aggregate(GroupBy $groupBy, FlowContext $context, Rows ...$batches): array
    {
        $output = self::batches(...$batches);

        foreach (NodeTranslatorMother::of($context->config)->steps(
            new Aggregate(NodeMother::read(), $groupBy),
        ) as $step) {
            $output = $step instanceof Processor
                ? $step->process($output, $context->backend())
                : self::transformed(type_instance_of(Transformer::class)->assert($step), $output, $context);
        }

        return iterator_to_array($output, preserve_keys: false);
    }

    /**
     * The group-by processor of $groupBy over the frame's grouping config, bound to $input when given.
     */
    public static function processor(
        GroupBy $groupBy,
        FlowContext $context,
        ?Schema $input = null,
    ): GroupByAggregationProcessor {
        $processor = new GroupByAggregationProcessor(
            $groupBy,
            $context->config->grouping,
            $context->config->randomValueGenerator(),
        );

        return $input === null
            ? $processor
            : type_instance_of(GroupByAggregationProcessor::class)->assert($processor->bind($input)->step);
    }

    /**
     * All aggregated batches merged into one Rows, for equality assertions.
     */
    public static function aggregated(GroupBy $groupBy, FlowContext $context, Rows ...$batches): Rows
    {
        $result = null;

        foreach (self::aggregate($groupBy, $context, ...$batches) as $batch) {
            $result = $result === null ? $batch : Rows::concat($result->schema(), $context->backend(), $result, $batch);
        }

        return $result ?? rows(schema());
    }

    /**
     * @return Generator<Rows>
     */
    public static function batches(Rows ...$batches): Generator
    {
        yield from $batches;
    }

    /**
     * @return list<array<string, mixed>> each row's group key, in row order
     */
    public static function keys(GroupBy $groupBy, Rows $rows, Schema $input): array
    {
        $ids = $groupBy->groupTable($input, new PhpBackend())->assign($rows);
        $values = $groupBy->keyValues($rows, $input, $ids->firstRows);
        $keys = [];

        /** @var int $id */
        foreach ($ids->ids->physicals() as $id) {
            $key = [];

            foreach ($values as $name => $byGroup) {
                $key[$name] = $byGroup[$id];
            }

            $keys[] = $key;
        }

        return $keys;
    }

    /**
     * @return list<int> each row's group id from one group table across the batches, in row order
     */
    public static function groupIds(GroupBy $groupBy, Schema $input, Rows ...$batches): array
    {
        $table = $groupBy->groupTable($input, new PhpBackend());
        $ids = [];

        foreach ($batches as $batch) {
            /** @var list<int> $batchIds */
            $batchIds = $table->assign($batch)->ids->physicals();
            $ids = [...$ids, ...$batchIds];
        }

        return $ids;
    }

    /**
     * @param Generator<Rows> $upstream
     *
     * @return Generator<Rows>
     */
    public static function transformed(Transformer $step, Generator $upstream, FlowContext $context): Generator
    {
        foreach ($upstream as $batch) {
            yield $step->transform($batch, $context->backend());
        }
    }
}

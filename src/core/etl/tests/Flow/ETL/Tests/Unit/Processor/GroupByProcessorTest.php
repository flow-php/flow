<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\GroupBy;
use Flow\ETL\Processor\GroupByProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\sum;

final class GroupByProcessorTest extends FlowTestCase
{
    public function test_groups_across_multiple_batches(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $processor = new GroupByProcessor($groupBy);

        $generator = (static function () {
            yield rows(row(str_entry('category', 'a'), int_entry('amount', 10)));
            yield rows(row(str_entry('category', 'a'), int_entry('amount', 20)));
            yield rows(row(str_entry('category', 'b'), int_entry('amount', 15)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[] = $groupRow;
            }
        }

        static::assertCount(2, $aggregated);

        $categoryA = array_values(array_filter($aggregated, static fn(array $r): bool => $r['category'] === 'a'))[0];
        $categoryB = array_values(array_filter($aggregated, static fn(array $r): bool => $r['category'] === 'b'))[0];

        static::assertSame(30, $categoryA['amount_sum']);
        static::assertSame(15, $categoryB['amount_sum']);
    }

    public function test_groups_and_aggregates_rows(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $processor = new GroupByProcessor($groupBy);

        $generator = (static function () {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 10)),
                row(str_entry('category', 'a'), int_entry('amount', 20)),
                row(str_entry('category', 'b'), int_entry('amount', 15)),
            );
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        $aggregated = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $groupRow) {
                $aggregated[] = $groupRow;
            }
        }

        static::assertCount(2, $aggregated);

        $categoryA = array_values(array_filter($aggregated, static fn(array $r): bool => $r['category'] === 'a'))[0];
        $categoryB = array_values(array_filter($aggregated, static fn(array $r): bool => $r['category'] === 'b'))[0];

        static::assertSame(30, $categoryA['amount_sum']);
        static::assertSame(15, $categoryB['amount_sum']);
    }

    public function test_handles_empty_input(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $processor = new GroupByProcessor($groupBy);

        $generator = (static function () {
            yield from [];
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));

        $total = 0;

        foreach ($result as $batch) {
            $total += $batch->count();
        }

        static::assertSame(0, $total);
    }
}

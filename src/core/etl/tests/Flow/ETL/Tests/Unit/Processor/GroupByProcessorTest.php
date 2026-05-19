<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\GroupBy;
use Flow\ETL\Processor\GroupByProcessor;
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

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);

        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-method-access
        $resultArray = $result[0]->toArray();
        // @mago-ignore analysis:mixed-argument
        static::assertCount(2, $resultArray);

        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-argument,mixed-array-access
        $categoryA = array_values(array_filter($resultArray, static fn($r) => $r['category'] === 'a'))[0];

        // @mago-ignore analysis:mixed-array-access
        static::assertEquals(30, $categoryA['amount_sum']);
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

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);

        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-method-access
        $resultArray = $result[0]->toArray();
        // @mago-ignore analysis:mixed-argument
        static::assertCount(2, $resultArray);

        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-argument,mixed-array-access
        $categoryA = array_values(array_filter($resultArray, static fn($r) => $r['category'] === 'a'))[0];
        // @mago-ignore analysis:mixed-assignment
        // @mago-ignore analysis:mixed-argument,mixed-array-access
        $categoryB = array_values(array_filter($resultArray, static fn($r) => $r['category'] === 'b'))[0];

        // @mago-ignore analysis:mixed-array-access
        static::assertEquals(30, $categoryA['amount_sum']);
        // @mago-ignore analysis:mixed-array-access
        static::assertEquals(15, $categoryB['amount_sum']);
    }

    public function test_handles_empty_input(): void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $processor = new GroupByProcessor($groupBy);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(1, $result);
        // @mago-ignore analysis:mixed-argument
        static::assertCount(0, $result[0]);
    }
}

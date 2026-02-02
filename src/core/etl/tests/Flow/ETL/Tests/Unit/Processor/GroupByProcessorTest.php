<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use function Flow\ETL\DSL\{flow_context, int_entry, ref, row, rows, str_entry, sum};
use Flow\ETL\GroupBy;
use Flow\ETL\Processor\GroupByProcessor;
use Flow\ETL\Tests\FlowTestCase;

final class GroupByProcessorTest extends FlowTestCase
{
    public function test_groups_across_multiple_batches() : void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $processor = new GroupByProcessor($groupBy);

        $generator = (static function () {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 10)),
            );
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 20)),
            );
            yield rows(
                row(str_entry('category', 'b'), int_entry('amount', 15)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);

        $resultArray = $result[0]->toArray();
        self::assertCount(2, $resultArray);

        $categoryA = array_values(array_filter($resultArray, fn ($r) => $r['category'] === 'a'))[0];

        self::assertEquals(30, $categoryA['amount_sum']);
    }

    public function test_groups_and_aggregates_rows() : void
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

        self::assertCount(1, $result);

        $resultArray = $result[0]->toArray();
        self::assertCount(2, $resultArray);

        $categoryA = array_values(array_filter($resultArray, fn ($r) => $r['category'] === 'a'))[0];
        $categoryB = array_values(array_filter($resultArray, fn ($r) => $r['category'] === 'b'))[0];

        self::assertEquals(30, $categoryA['amount_sum']);
        self::assertEquals(15, $categoryB['amount_sum']);
    }

    public function test_handles_empty_input() : void
    {
        $groupBy = new GroupBy(ref('category'));
        $groupBy->aggregate(sum(ref('amount')));

        $processor = new GroupByProcessor($groupBy);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(1, $result);
        self::assertCount(0, $result[0]);
    }
}

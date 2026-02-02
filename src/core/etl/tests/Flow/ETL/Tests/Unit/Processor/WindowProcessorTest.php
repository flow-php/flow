<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use function Flow\ETL\DSL\{flow_context, int_entry, rank, ref, row, rows, str_entry, window};
use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Tests\FlowTestCase;

final class WindowProcessorTest extends FlowTestCase
{
    public function test_applies_window_function() : void
    {
        $windowFunction = rank()->over(window()->partitionBy(ref('category'))->orderBy(ref('amount')->desc()));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(
                row(str_entry('category', 'a'), int_entry('amount', 100)),
                row(str_entry('category', 'a'), int_entry('amount', 200)),
                row(str_entry('category', 'b'), int_entry('amount', 150)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        self::assertCount(3, $allRows);
        self::assertArrayHasKey('rank', $allRows[0]);
    }

    public function test_handles_empty_input() : void
    {
        $windowFunction = rank()->over(window()->orderBy(ref('amount')));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        self::assertCount(0, $result);
    }

    public function test_handles_single_partition() : void
    {
        $windowFunction = rank()->over(window()->orderBy(ref('amount')->desc()));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(
                row(int_entry('amount', 300)),
                row(int_entry('amount', 100)),
                row(int_entry('amount', 200)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        self::assertCount(3, $allRows);
        self::assertContainsOnly('int', array_column($allRows, 'rank'));
    }

    public function test_processes_multiple_partitions() : void
    {
        $windowFunction = rank()->over(window()->partitionBy(ref('group'))->orderBy(ref('value')));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(
                row(str_entry('group', 'a'), int_entry('value', 10)),
                row(str_entry('group', 'a'), int_entry('value', 20)),
                row(str_entry('group', 'b'), int_entry('value', 5)),
                row(str_entry('group', 'b'), int_entry('value', 15)),
            );
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        self::assertCount(4, $allRows);

        $groupA = array_filter($allRows, fn ($r) => $r['group'] === 'a');
        $groupB = array_filter($allRows, fn ($r) => $r['group'] === 'b');

        self::assertCount(2, $groupA);
        self::assertCount(2, $groupB);
    }
}

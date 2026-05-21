<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Processor\WindowProcessor;
use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\rank;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\window;

final class WindowProcessorTest extends FlowTestCase
{
    public function test_applies_window_function(): void
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

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        /** @var list<array<array-key, mixed>> $allRows */
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(3, $allRows);
        static::assertArrayHasKey('rank', $allRows[0]);
    }

    public function test_handles_empty_input(): void
    {
        $windowFunction = rank()->over(window()->orderBy(ref('amount')));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield from [];
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));

        static::assertCount(0, $result);
    }

    public function test_handles_single_partition(): void
    {
        $windowFunction = rank()->over(window()->orderBy(ref('amount')->desc()));

        $processor = new WindowProcessor('rank', $windowFunction);

        $generator = (static function () {
            yield rows(row(int_entry('amount', 300)), row(int_entry('amount', 100)), row(int_entry('amount', 200)));
        })();

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(3, $allRows);
        // @mago-ignore analysis:deprecated-method
        static::assertContainsOnly('int', array_column($allRows, 'rank'));
    }

    public function test_processes_multiple_partitions(): void
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

        /** @var list<Rows> $result */
        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertCount(4, $allRows);

        $groupA = array_filter($allRows, static fn(array $r): bool => $r['group'] === 'a');
        $groupB = array_filter($allRows, static fn(array $r): bool => $r['group'] === 'b');

        static::assertCount(2, $groupA);
        static::assertCount(2, $groupB);
    }
}

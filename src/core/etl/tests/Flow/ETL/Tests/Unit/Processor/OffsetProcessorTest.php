<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Processor;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Processor\OffsetProcessor;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;

final class OffsetProcessorTest extends FlowTestCase
{
    public function test_offset_greater_than_total_rows_yields_nothing(): void
    {
        $processor = new OffsetProcessor(10);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $totalRows = 0;

        foreach ($result as $batch) {
            $totalRows += $batch->count();
        }

        static::assertSame(0, $totalRows);
    }

    public function test_offset_within_single_batch(): void
    {
        $processor = new OffsetProcessor(1);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)), row(int_entry('id', 3)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 2],
                ['id' => 3],
            ],
            $allRows,
        );
    }

    public function test_offset_zero_yields_all_rows(): void
    {
        $processor = new OffsetProcessor(0);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $totalRows = 0;

        foreach ($result as $batch) {
            $totalRows += $batch->count();
        }

        static::assertSame(2, $totalRows);
    }

    public function test_skips_first_n_rows(): void
    {
        $processor = new OffsetProcessor(2);

        $generator = (static function () {
            yield rows(row(int_entry('id', 1)), row(int_entry('id', 2)));
            yield rows(row(int_entry('id', 3)), row(int_entry('id', 4)));
        })();

        $result = iterator_to_array($processor->process($generator, flow_context()));
        $allRows = [];

        foreach ($result as $batch) {
            foreach ($batch->toArray() as $rowData) {
                $allRows[] = $rowData;
            }
        }

        static::assertEquals(
            [
                ['id' => 3],
                ['id' => 4],
            ],
            $allRows,
        );
    }

    public function test_throws_exception_for_negative_offset(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0');

        /** @phpstan-ignore-next-line */
        new OffsetProcessor(-1);
    }
}

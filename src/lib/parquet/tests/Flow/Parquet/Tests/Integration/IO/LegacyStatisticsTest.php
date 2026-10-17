<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\Reader;
use Flow\Parquet\Reader\RowSelection;
use Flow\Parquet\Tests\Context\FilePlan;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\DSL\parquet_column;

final class LegacyStatisticsTest extends TestCase
{
    public function test_a_file_without_column_orders_has_no_trusted_statistics(): void
    {
        $metadata = Reader::php()->read(__DIR__
        . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet')->metadata();

        foreach ($metadata->schema()->columnsFlat() as $column) {
            static::assertSame(ColumnOrder::Absent, $metadata->columnOrder($column));
            static::assertFalse(ColumnOrder::Absent->isTrusted($column));
        }
    }

    public function test_a_file_without_column_orders_keeps_every_row_group(): void
    {
        $file = Reader::php()->read(__DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet');

        static::assertCount(50, $file->metadata()->rowGroups()->all());
        static::assertEquals(RowSelection::all(5000), FilePlan::of($file, parquet_column('id')->equals(1))->selection);
        static::assertEquals(
            RowSelection::all(5000),
            FilePlan::of($file, parquet_column('name')->equals('name-0'))->selection,
        );
    }
}

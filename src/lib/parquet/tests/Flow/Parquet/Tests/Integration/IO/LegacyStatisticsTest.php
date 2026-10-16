<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\Reader;
use PHPUnit\Framework\TestCase;

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
}

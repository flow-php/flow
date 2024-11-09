<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetFile\{ColumnPageHeader};
use Flow\Parquet\{Reader};
use PHPUnit\Framework\TestCase;

final class ReaderTest extends TestCase
{
    public function test_reading_columns_with_multiple_data_pages() : void
    {
        // File generated with  https://gist.github.com/norberttech/325df9166bbdb33e18dffa94c1a033c4
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/multiple_pages.parquet');

        $rows = 0;

        foreach ($file->values() as $row) {
            foreach ($row as $column => $value) {
                self::assertNotNull($value);
            }
            $rows++;
        }

        $headers = \iterator_to_array($file->pageHeaders());

        self::assertCount(79, $headers);
        self::assertSame(128, $headers[0]->pageHeader->dataValuesCount());
        self::assertSame(16, $headers[78]->pageHeader->dataValuesCount());
        self::assertSame(10_000, \array_sum(\array_map(static fn (ColumnPageHeader $header) => $header->pageHeader->dataValuesCount(), $headers)));
        self::assertSame(10_000, $rows);
    }

    public function test_reading_required_columns() : void
    {
        // File generated with https://gist.github.com/norberttech/01322f61dca77cfde5161e31e94463ef
        $reader = new Reader();
        $file = $reader->read(__DIR__ . '/Fixtures/columns.required.parquet');

        $rows = 0;

        foreach ($file->values() as $row) {
            foreach ($row as $column => $value) {
                self::assertNotNull($value, "Column {$column} is null");
            }
            $rows++;
        }

        self::assertSame(100, $rows);
    }
}

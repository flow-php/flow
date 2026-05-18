<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Page\ColumnPageHeader;
use Flow\Parquet\Reader;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_sum;
use function iterator_to_array;

class ReaderTest extends ParquetIntegrationTestCase
{
    #[DataProvider('engine_provider')]
    public function test_reading_columns_with_multiple_data_pages(ParquetEngine $engine): void
    {
        // File generated with  https://gist.github.com/norberttech/325df9166bbdb33e18dffa94c1a033c4
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/multiple_pages.parquet');

        $rows = 0;

        foreach ($file->values() as $row) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $column => $value) {
                static::assertNotNull($value);
            }
            $rows++;
        }

        $headers = iterator_to_array($file->pageHeaders());

        static::assertCount(79, $headers);
        static::assertSame(128, $headers[0]->pageHeader->dataValuesCount());
        static::assertSame(16, $headers[78]->pageHeader->dataValuesCount());
        static::assertSame(
            10_000,
            array_sum(array_map(
                static fn(ColumnPageHeader $header) => $header->pageHeader->dataValuesCount() ?? 0,
                $headers,
            )),
        );
        static::assertSame(10_000, $rows);
    }

    #[DataProvider('engine_provider')]
    public function test_reading_required_columns(ParquetEngine $engine): void
    {
        // File generated with https://gist.github.com/norberttech/01322f61dca77cfde5161e31e94463ef
        $reader = new Reader(engine: $engine);
        $file = $reader->read(__DIR__ . '/Fixtures/columns.required.parquet');

        $rows = 0;

        foreach ($file->values() as $row) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($row as $column => $value) {
                static::assertNotNull($value, "Column {$column} is null");
            }
            $rows++;
        }

        static::assertSame(100, $rows);
    }
}

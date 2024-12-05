<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use function Flow\ETL\DSL\{config, flow_context};
use Flow\ETL\Adapter\Parquet\ParquetExtractor;
use Flow\Filesystem\Path;
use Flow\Parquet\{Reader};
use PHPUnit\Framework\TestCase;

final class PaginationTest extends TestCase
{
    public function test_multifile_pagination_from_beginning() : void
    {
        $extractor = (new ParquetExtractor(
            Path::realpath(__DIR__ . '/Fixtures/Pagination/*.parquet'),
        ))->withOffset(0);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $extractedRows += $rows->count();
        }

        self::assertSame(
            (1000 + 500 + 350 + 2000 + 15),
            $extractedRows
        );
    }

    public function test_multifile_pagination_from_middle() : void
    {
        $extractor = (new ParquetExtractor(
            Path::realpath(__DIR__ . '/Fixtures/Pagination/*.parquet'),
        ))->withOffset(2500);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $extractedRows += $rows->count();
        }

        self::assertSame(
            (1000 + 500 + 350 + 2000 + 15) - 2500,
            $extractedRows
        );
    }

    public function test_multifile_pagination_from_middle_partitioned() : void
    {
        $extractor = (new ParquetExtractor(
            Path::realpath(__DIR__ . '/Fixtures/Pagination/partitioned/date=*/*.parquet'),
        ))->withOffset(2500);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $extractedRows += $rows->count();
        }

        self::assertSame(
            (1000 + 500 + 350 + 2000 + 15) - 2500,
            $extractedRows
        );
    }

    public function test_multifile_pagination_from_offset_bigger_than_total_rows() : void
    {
        $extractor = (new ParquetExtractor(
            Path::realpath(__DIR__ . '/Fixtures/Pagination/*.parquet'),
        ))->withOffset(10_000);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $rows) {
            $extractedRows += $rows->count();
        }

        self::assertSame(
            0,
            $extractedRows
        );
    }

    public function test_reading_file_from_given_offset() : void
    {
        $totalRows = (new Reader())->read(__DIR__ . '/Fixtures/orders_1k.parquet')->metadata()->rowsNumber();

        $extractor = (new ParquetExtractor(
            Path::realpath(__DIR__ . '/Fixtures/orders_1k.parquet'),
        ))->withOffset($totalRows - 100);

        self::assertCount(
            100,
            \iterator_to_array($extractor->extract(flow_context(config())))
        );
    }
}

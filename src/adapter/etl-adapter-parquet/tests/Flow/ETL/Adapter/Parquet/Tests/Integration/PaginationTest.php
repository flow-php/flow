<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Reader;

use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\Filesystem\DSL\path_real;

final class PaginationTest extends FlowTestCase
{
    public function test_multifile_pagination_from_beginning(): void
    {
        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/Pagination/*.parquet'))->withOffset(0);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(1000 + 500 + 350 + 2000 + 15, $extractedRows);
    }

    public function test_multifile_pagination_from_middle(): void
    {
        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/Pagination/*.parquet'))->withOffset(2500);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(1000 + 500 + 350 + 2000 + 15 - 2500, $extractedRows);
    }

    public function test_multifile_pagination_from_middle_partitioned(): void
    {
        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/Pagination/partitioned/date=*/*.parquet'))
            ->withOffset(2500);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(1000 + 500 + 350 + 2000 + 15 - 2500, $extractedRows);
    }

    public function test_multifile_pagination_from_offset_bigger_than_total_rows(): void
    {
        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/Pagination/*.parquet'))->withOffset(10_000);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(0, $extractedRows);
    }

    public function test_reading_file_from_given_offset(): void
    {
        $totalRows = (new Reader())
            ->read(__DIR__ . '/Fixtures/orders_1k.parquet')
            ->metadata()
            ->rowsNumber();

        $extractor = from_parquet(path_real(__DIR__ . '/Fixtures/orders_1k.parquet'))->withOffset($totalRows - 100);

        $extractedRows = 0;

        foreach ($extractor->extract(flow_context(config())) as $batch) {
            $extractedRows += $batch->count();
        }

        static::assertSame(100, $extractedRows);
    }
}

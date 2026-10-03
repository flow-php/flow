<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet\Tests\Integration;

use Flow\ETL\Rows;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Parquet\Reader;

use function array_map;
use function array_merge;
use function Flow\ETL\Adapter\Parquet\from_parquet;
use function Flow\ETL\Adapter\Parquet\to_parquet;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\df;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\from_array;
use function Flow\Filesystem\DSL\memory_filesystem;
use function Flow\Filesystem\DSL\path_real;
use function iterator_to_array;

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

    public function test_an_offset_spanning_two_files_and_a_limit_inside_the_third_read_the_same_twice(): void
    {
        $filesystem = memory_filesystem();

        foreach (['a' => [1, 2, 3], 'b' => [4, 5, 6], 'c' => [7, 8, 9]] as $name => $ids) {
            df()
                ->read(from_array(array_map(static fn(int $id): array => ['id' => $id], $ids)))
                ->write(to_parquet('memory://window/' . $name . '.parquet', filesystem: $filesystem))
                ->run();
        }

        $extractor = from_parquet('memory://window/*.parquet', filesystem: $filesystem)->withOffset(4);
        $read = static fn(): array => array_merge(...array_map(
            static fn(Rows $rows): array => $rows->column('id')->values(),
            iterator_to_array($extractor->extract(flow_context(), limit: 4), false),
        ));

        static::assertSame([5, 6, 7, 8], $read());
        static::assertSame([5, 6, 7, 8], $read());
    }
}

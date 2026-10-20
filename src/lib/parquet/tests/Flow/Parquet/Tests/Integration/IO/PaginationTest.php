<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Query;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Tests\Mother\ParquetEngineMother;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function iterator_to_array;
use function range;

class PaginationTest extends ParquetIntegrationTestCase
{
    public static function engine_with_offset_provider(): Generator
    {
        $cases = [
            [6000, 10, 0],
            [4900, 100, 100],
            [0, null, 5000],
            [4999, 2, 1],
            [0, 2, 2],
        ];

        foreach (self::engine_provider() as $engineName => $engineArgs) {
            foreach ($cases as $case) {
                yield $engineName . ' offset ' . $case[0] . ' limit ' . ($case[1] ?? 'null') => [
                    $engineArgs[0],
                    $case[0],
                    $case[1],
                    $case[2],
                ];
            }
        }
    }

    #[DataProvider('engine_provider')]
    public function test_offset_past_first_page_of_multi_page_column(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $path = TestParquetFile::path($this);

        Writer::php(options: Options::default()->set(Option::PAGE_SIZE_BYTES, 100)->set(
            Option::PAGE_SIZE_CHECK_INTERVAL,
            1,
        ))->write(
            $path,
            Schema::with(FlatColumn::int64('id')),
            array_map(static fn(int $i): array => ['id' => $i], range(0, 99)),
        );

        $file = (new Reader(engine: $engine))->read($path);

        static::assertSame(
            array_map(static fn(int $i): array => ['id' => $i], range(30, 99)),
            iterator_to_array($file->values(new Query(['id'], offset: 30))),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_last_100_rows(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $totalRows = (new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->rowsNumber();

        $file = (new Reader(engine: $engine))->read($path);

        static::assertEquals(
            array_merge(...array_map(
                static fn(int $i): array => [['id' => $i]],
                range($totalRows - 100, $totalRows - 1),
            )),
            iterator_to_array($file->values(new Query(['id'], offset: $totalRows - 100))),
        );
    }

    #[DataProvider('engine_with_offset_provider')]
    public function test_setting_offset_larger_than_file(
        string $engineClass,
        int $offset,
        ?int $limit,
        int $results,
    ): void {
        $engine = ParquetEngineMother::create($engineClass);
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $file = (new Reader(engine: $engine))->read($path);

        static::assertCount(
            $results,
            iterator_to_array($file->values(new Query(['id'], offset: $offset, limit: $limit))),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_columns_refuses_a_batch_size_below_one(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Batch size must be greater than 0');

        iterator_to_array(
            (new Reader(engine: $engine))
                ->read(__DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet')
                ->columns(new Query(['id']), 0),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_columns_with_a_limit_of_zero_read_no_row(string $engineClass): void
    {
        $file = (new Reader(engine: ParquetEngineMother::create($engineClass)))->read(__DIR__
        . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet');

        static::assertSame([], iterator_to_array($file->columns(new Query(['id'], limit: 0), 2)));
    }

    #[DataProvider('engine_provider')]
    public function test_columns_refuses_an_unknown_column(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "nope" does not exist');

        iterator_to_array(
            (new Reader(engine: $engine))
                ->read(__DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet')
                ->columns(new Query(['nope']), 2),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_values_equal_the_zip_of_columns(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $file = (new Reader(engine: $engine))->read(__DIR__
        . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet');

        $zipped = [];

        $query = new Query(['id', 'name'], offset: 1020, limit: 7);

        foreach ($file->columns($query, 3) as $chunk) {
            // @mago-ignore analysis:mixed-assignment
            foreach ($chunk['id'] as $i => $id) {
                $zipped[] = ['id' => $id, 'name' => $chunk['name'][$i]];
            }
        }

        static::assertSame($zipped, iterator_to_array($file->values($query), false));
        static::assertCount(7, $zipped);
    }

    #[DataProvider('engine_provider')]
    public function test_simple_pagination_on_small_row_group_size(string $engineClass): void
    {
        $engine = ParquetEngineMother::create($engineClass);
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        // Uncomment only to apply changes to the dataset
        // $this->generateDataset($path);

        $file = (new Reader(engine: $engine))->read($path);

        static::assertEquals(
            array_merge(...array_map(static fn(int $i): array => [['id' => $i]], range(1020, 1029))),
            iterator_to_array($file->values(new Query(['id'], offset: 1020, limit: 10))),
        );
    }
}

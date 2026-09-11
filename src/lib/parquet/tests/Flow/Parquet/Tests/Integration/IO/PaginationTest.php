<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use DateTimeImmutable;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetEngine;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Reader;
use Flow\Parquet\Tests\Context\TestParquetFile;
use Flow\Parquet\Writer;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;

use function array_map;
use function array_merge;
use function file_exists;
use function Flow\ETL\DSL\generate_random_int;
use function iterator_to_array;
use function range;
use function unlink;

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
    public function test_offset_past_first_page_of_multi_page_column(ParquetEngine $engine): void
    {
        $path = TestParquetFile::path($this);

        Writer::php(options: Options::default()->set(Option::PAGE_SIZE_BYTES, 100)->set(
            Option::PAGE_SIZE_CHECK_INTERVAL,
            1,
        ))->write(
            $path,
            Schema::with(FlatColumn::int64('id')),
            array_map(static fn(int $i): array => ['id' => $i], range(0, 99)),
        );

        static::assertSame(
            array_map(static fn(int $i): array => ['id' => $i], range(30, 99)),
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(['id'], offset: 30),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_reading_last_100_rows(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $totalRows = (new Reader(engine: $engine))
            ->read($path)
            ->metadata()
            ->rowsNumber();

        static::assertEquals(
            array_merge(...array_map(
                static fn(int $i): array => [['id' => $i]],
                range($totalRows - 100, $totalRows - 1),
            )),
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(['id'], offset: $totalRows - 100),
            ),
        );
    }

    #[DataProvider('engine_with_offset_provider')]
    public function test_setting_offset_larger_than_file(
        ParquetEngine $engine,
        int $offset,
        ?int $limit,
        int $results,
    ): void {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        static::assertCount(
            $results,
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(['id'], offset: $offset, limit: $limit),
            ),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_setting_setting_limit_to_negative(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        iterator_to_array(
            (new Reader(engine: $engine))
                ->read($path)
                ->values(['id'], limit: -2),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_setting_setting_offset_to_negative(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0');

        iterator_to_array(
            (new Reader(engine: $engine))
                ->read($path)
                ->values(['id'], offset: -2, limit: 2),
        );
    }

    #[DataProvider('engine_provider')]
    public function test_simple_pagination_on_small_row_group_size(ParquetEngine $engine): void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        // Uncomment only to apply changes to the dataset
        // $this->generateDataset($path);

        static::assertEquals(
            array_merge(...array_map(static fn(int $i): array => [['id' => $i]], range(1020, 1029))),
            iterator_to_array(
                (new Reader(engine: $engine))
                    ->read($path)
                    ->values(['id'], offset: 1020, limit: 10),
            ),
        );
    }

    private function _generateDataset(string $path): void
    {
        $writer = new Writer(options: Options::default()->set(Option::ROW_GROUP_SIZE_BYTES, 1024)->set(
            Option::ROW_GROUP_SIZE_CHECK_INTERVAL,
            100,
        ));
        $schema = Schema::with(
            FlatColumn::int64('id'),
            FlatColumn::string('name'),
            FlatColumn::boolean('active'),
            FlatColumn::dateTime('created_at'),
            NestedColumn::list('list_of_int', ListElement::int32()),
            NestedColumn::map('map_of_int_string', MapKey::int32(), MapValue::string()),
            NestedColumn::struct('struct', [
                FlatColumn::int64('id'),
                FlatColumn::string('name'),
                FlatColumn::boolean('active'),
                FlatColumn::dateTime('created_at'),
                NestedColumn::list('list_of_int', ListElement::int32()),
                NestedColumn::map('map_of_int_string', MapKey::int32(), MapValue::string()),
            ]),
        );

        $inputData = array_merge(...array_map(
            static fn(int $i): array => [
                [
                    'id' => $i,
                    'name' => 'name-' . $i,
                    'active' => ($i % 2) === 0,
                    'created_at' => new DateTimeImmutable('2024-01-01 + ' . $i . ' days'),
                    'list_of_int' => array_map(static fn(int $i) => $i, range(1, generate_random_int(2, 10))),
                    'map_of_int_string' => array_merge(...array_map(
                        static fn(int $i) => [$i => 'value-' . $i],
                        range(1, generate_random_int(2, 10)),
                    )),
                    'struct' => [
                        'id' => $i,
                        'name' => 'name-' . $i,
                        'active' => ($i % 2) === 0,
                        'created_at' => new DateTimeImmutable('2024-01-01 + ' . $i . ' days'),
                        'list_of_int' => array_map(static fn(int $i) => $i, range(1, generate_random_int(2, 10))),
                        'map_of_int_string' => array_merge(...array_map(
                            static fn(int $i) => [$i => 'value-' . $i],
                            range(1, generate_random_int(2, 10)),
                        )),
                    ],
                ],
            ],
            range(0, 4999),
        ));

        if (file_exists($path)) {
            unlink($path);
        }

        $writer->write($path, $schema, $inputData);
    }
}

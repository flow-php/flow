<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Integration\IO;

use function Flow\ETL\DSL\generate_random_int;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn};
use Flow\Parquet\{Option, Options, Reader, Writer};
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class PaginationTest extends TestCase
{
    public function test_reading_last_100_rows() : void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $totalRows = (new Reader())->read($path)->metadata()->rowsNumber();

        self::assertEquals(
            \array_merge(
                ...\array_map(
                    static function (int $i) : array {
                        return [['id' => $i]];
                    },
                    \range($totalRows - 100, $totalRows - 1)
                )
            ),
            \iterator_to_array((new Reader())->read($path)->values(['id'], offset: $totalRows - 100))
        );
    }

    #[TestWith([6000, 10, 0])]
    #[TestWith([4900, 100, 100])]
    #[TestWith([0, null, 5000])]
    #[TestWith([4999, 2, 1])]
    #[TestWith([0, 2, 2])]
    public function test_setting_offset_larger_than_file(int $offset, ?int $limit, int $results) : void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        self::assertCount(
            $results,
            \iterator_to_array((new Reader())->read($path)->values(['id'], offset: $offset, limit: $limit))
        );
    }

    public function test_setting_setting_limit_to_negative() : void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Limit must be greater than 0');

        \iterator_to_array((new Reader())->read($path)->values(['id'], limit: -2));
    }

    public function test_setting_setting_offset_to_negative() : void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Offset must be greater than or equal to 0');

        \iterator_to_array((new Reader())->read($path)->values(['id'], offset: -2, limit: 2));
    }

    public function test_simple_pagination_on_small_row_group_size() : void
    {
        $path = __DIR__ . '/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet';

        // Uncomment only to apply changes to the dataset
        // $this->generateDataset($path);

        self::assertEquals(
            \array_merge(
                ...\array_map(
                    static function (int $i) : array {
                        return [['id' => $i]];
                    },
                    \range(1020, 1029)
                )
            ),
            \iterator_to_array((new Reader())->read($path)->values(['id'], offset: 1020, limit: 10))
        );
    }

    private function generateDataset(string $path) : void
    {
        $writer = new Writer(
            options: Options::default()
                ->set(Option::ROW_GROUP_SIZE_BYTES, 1024)
                ->set(Option::ROW_GROUP_SIZE_CHECK_INTERVAL, 100)
        );
        $schema = Schema::with(
            FlatColumn::int64('id'),
            FlatColumn::string('name'),
            FlatColumn::boolean('active'),
            FlatColumn::dateTime('created_at'),
            NestedColumn::list('list_of_int', Schema\ListElement::int32()),
            NestedColumn::map('map_of_int_string', Schema\MapKey::int32(), Schema\MapValue::string()),
            NestedColumn::struct('struct', [
                FlatColumn::int64('id'),
                FlatColumn::string('name'),
                FlatColumn::boolean('active'),
                FlatColumn::dateTime('created_at'),
                NestedColumn::list('list_of_int', Schema\ListElement::int32()),
                NestedColumn::map('map_of_int_string', Schema\MapKey::int32(), Schema\MapValue::string()),
            ])
        );

        $inputData = \array_merge(...\array_map(static function (int $i) : array {
            return [
                [
                    'id' => $i,
                    'name' => 'name-' . $i,
                    'active' => $i % 2 === 0,
                    'created_at' => new \DateTimeImmutable('2024-01-01 + ' . $i . ' days'),
                    'list_of_int' => \array_map(
                        static fn (int $i) => $i,
                        \range(1, generate_random_int(2, 10))
                    ),
                    'map_of_int_string' => \array_merge(
                        ...\array_map(
                            static fn (int $i) => [$i => 'value-' . $i],
                            \range(1, generate_random_int(2, 10))
                        )
                    ),
                    'struct' => [
                        'id' => $i,
                        'name' => 'name-' . $i,
                        'active' => $i % 2 === 0,
                        'created_at' => new \DateTimeImmutable('2024-01-01 + ' . $i . ' days'),
                        'list_of_int' => \array_map(
                            static fn (int $i) => $i,
                            \range(1, generate_random_int(2, 10))
                        ),
                        'map_of_int_string' => \array_merge(
                            ...\array_map(
                                static fn (int $i) => [$i => 'value-' . $i],
                                \range(1, generate_random_int(2, 10))
                            )
                        ),
                    ],
                ],
            ];
        }, \range(0, 4999)));

        if (\file_exists($path)) {
            \unlink($path);
        }

        $writer->write($path, $schema, $inputData);
    }
}

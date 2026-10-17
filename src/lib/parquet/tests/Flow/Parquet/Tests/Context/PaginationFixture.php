<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateTimeImmutable;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Writer;

use function array_map;
use function array_merge;
use function file_exists;
use function Flow\ETL\DSL\generate_random_int;
use function range;
use function unlink;

/**
 * Writes Integration/IO/Fixtures/pagination_row_group_1kb_5k_rows.snappy.parquet: 5000 rows in row groups of 1 KB.
 */
final class PaginationFixture
{
    public static function write(string $path): void
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

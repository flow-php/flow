<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Context;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\ListElement;
use Flow\Parquet\ParquetFile\Schema\MapKey;
use Flow\Parquet\ParquetFile\Schema\MapValue;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;

use function array_fill_keys;
use function array_map;

/**
 * Every lib FlatColumn factory and each nested shape, with three rows: values at the edges, nulls, empty values.
 */
final class EveryType
{
    /**
     * @return list<array<string, mixed>>
     */
    public static function rows(): array
    {
        return [
            [
                'boolean' => true,
                'int32' => -2_147_483_648,
                'int64' => PHP_INT_MAX,
                'float' => 0.1,
                'double' => 1 / 3,
                'string' => 'zażółć',
                'json' => '{"a":1}',
                'enum' => 'RED',
                'uuid' => '0190a6b0-6b1f-7c5e-9a3b-0123456789ab',
                'date' => new DateTimeImmutable('1969-12-31 23:00:00', new DateTimeZone('Europe/Warsaw')),
                'datetime' => new DateTimeImmutable('1960-01-01 12:34:56.789012', new DateTimeZone('America/New_York')),
                'time' => new DateInterval('PT25H3M4S'),
                'decimal' => 12_345_678.91,
                'decimal38' => 12_345_678_901_234_567_890.123456789,
                'fixed4' => "\x00\x01\x02\x03",
                'list_int' => [1, null, 3],
                'list_datetime' => [new DateTimeImmutable('2024-02-29 00:00:00.5')],
                'map_string_date' => ['a' => new DateTimeImmutable('2000-01-01'), 'b' => null],
                'struct' => [
                    'id' => '0190a6b0-6b1f-7c5e-9a3b-0123456789ab',
                    'at' => new DateInterval('PT0S'),
                    'amount' => -0.01,
                ],
            ],
            array_fill_keys(array_map(static fn($column) => $column->name(), self::schema()->columns()), null),
            [
                'boolean' => false,
                'int32' => 0,
                'int64' => -1,
                'float' => -2.5e10,
                'double' => -0.0,
                'string' => '',
                'json' => '[]',
                'enum' => '',
                'uuid' => '00000000-0000-0000-0000-000000000000',
                'date' => new DateTimeImmutable('2262-04-11'),
                'datetime' => new DateTimeImmutable('@0'),
                'time' => new DateInterval('PT23H59M59S'),
                'decimal' => -99_999_999.99,
                'decimal38' => 0.000000001,
                'fixed4' => "\xFF\xFF\xFF\xFF",
                'list_int' => [],
                'list_datetime' => null,
                'map_string_date' => [],
                'struct' => ['id' => null, 'at' => null, 'amount' => null],
            ],
        ];
    }

    public static function schema(): Schema
    {
        return Schema::with(
            FlatColumn::boolean('boolean'),
            FlatColumn::int32('int32'),
            FlatColumn::int64('int64'),
            FlatColumn::float('float'),
            FlatColumn::double('double'),
            FlatColumn::string('string'),
            FlatColumn::json('json'),
            FlatColumn::enum('enum'),
            FlatColumn::uuid('uuid'),
            FlatColumn::date('date'),
            FlatColumn::dateTime('datetime'),
            FlatColumn::time('time'),
            FlatColumn::decimal('decimal', 10, 2),
            FlatColumn::decimal('decimal38', 38, 9),
            FlatColumn::fixedSizeByteArray('fixed4', 4),
            NestedColumn::list('list_int', ListElement::int64()),
            NestedColumn::list('list_datetime', ListElement::datetime()),
            NestedColumn::map('map_string_date', MapKey::string(), MapValue::date()),
            NestedColumn::struct('struct', [
                FlatColumn::uuid('id'),
                FlatColumn::time('at'),
                FlatColumn::decimal('amount'),
            ]),
        );
    }
}

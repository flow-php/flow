<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Values\Grouping;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Tests\Mother\ColumnMother;
use Flow\ETL\Values\Grouping\KeyEncoder;
use Flow\ETL\Values\Schema\Definitions;
use Flow\Filesystem\Partition;
use Flow\Types\Type;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function array_map;
use function array_merge;
use function array_unique;
use function count;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

use const NAN;

final class KeyEncoderTest extends FlowTestCase
{
    public static function sharedKeys(): Generator
    {
        yield 'two nan payloads' => [type_float(), [NAN, -NAN]];
        yield 'a date' => [type_date(), [new DateTimeImmutable('2026-11-20'), new DateTimeImmutable('2026-11-20')]];
        yield 'one instant in utc and in +02:00' => [
            type_datetime(),
            [new DateTimeImmutable('2026-11-20T10:00:00+00:00'), new DateTimeImmutable('2026-11-20T12:00:00+02:00')],
        ];
    }

    /**
     * @param Type<mixed> $type
     * @param list<mixed> $values
     */
    #[DataProvider('sharedKeys')]
    public function test_one_key_gives_one_partition_value(Type $type, array $values): void
    {
        static::assertCount(
            1,
            array_unique(KeyEncoder::ofColumns([ColumnMother::of(
                Definitions::of('k', $type),
                $values,
            )], count($values))),
        );
        static::assertCount(
            1,
            array_unique(array_map(static fn(mixed $value): string => Partition::fromValue(
                'k',
                $type,
                $value,
            ), $values)),
        );
    }

    public function test_zero_and_negative_zero_are_one_key_but_two_partition_values(): void
    {
        static::assertCount(
            1,
            array_unique(KeyEncoder::ofColumns([ColumnMother::of(Definitions::of('k', type_float()), [0.0, -0.0])], 2)),
        );
        static::assertSame(['0.0', '-0.0'], [
            Partition::fromValue('k', type_float(), 0.0),
            Partition::fromValue('k', type_float(), -0.0),
        ]);
    }

    public function test_an_empty_batch_has_no_keys(): void
    {
        static::assertSame([], (new KeyEncoder(['k']))->of(rows(schema(int_schema('k')))));
    }

    public function test_equal_rows_share_a_key_and_others_do_not(): void
    {
        $keys = (new KeyEncoder(['k', 'l']))->of(array_to_rows(
            [['k' => 'a', 'l' => [1, 2]], ['k' => 'a', 'l' => [1, 2]], ['k' => 'a', 'l' => [2, 1]]],
            schema(str_schema('k'), list_schema('l', type_list(type_integer()))),
        ));

        static::assertSame($keys[0], $keys[1]);
        static::assertNotSame($keys[0], $keys[2]);
    }

    public function test_zero_and_negative_zero_are_one_key(): void
    {
        $keys = (new KeyEncoder(['k']))->of(array_to_rows([
            ['k' => 0.0],
            ['k' => -0.0],
            ['k' => 1.5],
        ], schema(float_schema('k'))));

        static::assertSame($keys[0], $keys[1]);
        static::assertNotSame($keys[0], $keys[2]);
    }

    public function test_a_float_keeps_every_digit(): void
    {
        $keys = (new KeyEncoder(['k']))->of(array_to_rows([
            ['k' => 0.1 + 0.2],
            ['k' => 0.3],
        ], schema(float_schema('k'))));

        static::assertNotSame($keys[0], $keys[1]);
    }

    public function test_values_of_different_types_never_share_a_key(): void
    {
        static::assertCount(
            10,
            array_unique(array_merge(
                (new KeyEncoder(['k']))->of(array_to_rows([['k' => 1], ['k' => 0]], schema(int_schema('k')))),
                (new KeyEncoder(['k']))->of(array_to_rows([['k' => 1.0]], schema(float_schema('k')))),
                (new KeyEncoder(['k']))->of(array_to_rows([
                    ['k' => '1'],
                    ['k' => '1.0'],
                    ['k' => ''],
                    ['k' => '0'],
                ], schema(str_schema('k')))),
                (new KeyEncoder(['k']))->of(array_to_rows([['k' => true], ['k' => false]], schema(bool_schema('k')))),
                (new KeyEncoder(['k']))->of(array_to_rows([['k' => null]], schema(int_schema('k', nullable: true)))),
            )),
        );
    }

    public function test_a_value_never_runs_into_the_next_one(): void
    {
        $keys = (new KeyEncoder(['a', 'b']))->of(array_to_rows(
            [['a' => 'ab', 'b' => ''], ['a' => 'a', 'b' => 'b']],
            schema(str_schema('a'), str_schema('b')),
        ));

        static::assertNotSame($keys[0], $keys[1]);
    }

    public function test_a_column_the_batch_lacks_is_a_null_where_the_bound_schema_declares_it_nullable(): void
    {
        $declared = schema(int_schema('k'), int_schema('missing', nullable: true));

        static::assertSame(
            (new KeyEncoder(['k', 'missing']))->of(array_to_rows([['k' => 1, 'missing' => null]], $declared)),
            (new KeyEncoder(['k', 'missing'], declared: $declared))->of(array_to_rows([[
                'k' => 1,
            ]], schema(int_schema('k')))),
        );
    }

    public function test_a_column_the_batch_lacks_without_a_nullable_declaration_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Column "missing" does not exist.');

        (new KeyEncoder(['k', 'missing'], declared: schema(int_schema('k'), int_schema('missing'))))->of(array_to_rows(
            [[
                'k' => 1,
            ]],
            schema(int_schema('k')),
        ));
    }

    public function test_a_date_is_keyed_by_its_day(): void
    {
        $keys = (new KeyEncoder(['d']))->of(array_to_rows([
            ['d' => new DateTimeImmutable('2026-01-01')],
            ['d' => new DateTimeImmutable('2026-01-01')],
            ['d' => new DateTimeImmutable('2026-01-02')],
        ], schema(date_schema('d'))));

        static::assertSame($keys[0], $keys[1]);
        static::assertNotSame($keys[0], $keys[2]);
    }

    public function test_a_column_cast_to_a_type_keys_like_a_column_of_that_type(): void
    {
        static::assertSame(
            (new KeyEncoder(['k']))->of(array_to_rows([['k' => 1.0]], schema(float_schema('k')))),
            (new KeyEncoder(['k'], [type_float()]))->of(array_to_rows([['k' => 1]], schema(int_schema('k')))),
        );
        static::assertSame(
            (new KeyEncoder(['k']))->of(array_to_rows([['k' => 1]], schema(int_schema('k')))),
            (new KeyEncoder(['k'], [type_integer()]))->of(array_to_rows([['k' => '1']], schema(str_schema('k')))),
        );
    }

    public function test_nans_share_a_key(): void
    {
        $keys = (new KeyEncoder(['k']))->of(array_to_rows([['k' => NAN], ['k' => NAN]], schema(float_schema('k'))));

        static::assertSame($keys[0], $keys[1]);
    }

    #[TestWith(["\0i1", 7, 0, 5])]
    #[TestWith(["\0i1", 7, 8, 0])]
    #[TestWith(["\0i1", 7, 24, 1])]
    #[TestWith(["\0s3:abc", 7, 0, 5])]
    #[TestWith(["\0s3:abc", 7, 8, 0])]
    #[TestWith(["\0s3:abc", 7, 24, 2])]
    #[TestWith(["\0n\0t", 7, 0, 0])]
    #[TestWith(["\0n\0t", 7, 8, 2])]
    #[TestWith(["\0n\0t", 7, 24, 5])]
    #[TestWith(['', 7, 0, 0])]
    #[TestWith(['', 7, 8, 1])]
    #[TestWith(['', 7, 24, 1])]
    #[TestWith(["\0i1", 4294967296, 0, 2087460912])]
    #[TestWith(["\0s3:abc", 4294967296, 8, 2786302190])]
    #[TestWith(['', 4294967296, 24, 1183664511])]
    public function test_the_bucket_of_a_key_is_eight_hex_digits_of_its_xxh128_at_the_offset(
        string $key,
        int $buckets,
        int $hashOffset,
        int $bucket,
    ): void {
        static::assertSame($bucket, KeyEncoder::bucketOf($key, $buckets, $hashOffset));
    }

    public function test_an_int_and_a_float_of_one_value_are_one_key_under_a_float_key_type(): void
    {
        $declared = schema(float_schema('k'));

        static::assertSame(
            ["\x03\x00\x00\x00\x00\x00\x00\xF0\x3F"],
            (new KeyEncoder(['k'], declared: $declared))->of(array_to_rows([['k' => 1]], schema(int_schema('k')))),
        );
        static::assertSame(
            ["\x03\x00\x00\x00\x00\x00\x00\xF0\x3F"],
            (new KeyEncoder(['k'], declared: $declared))->of(array_to_rows([['k' => 1.0]], $declared)),
        );
    }

    public function test_a_column_the_batch_lacks_under_a_nullable_declaration_keys_as_null(): void
    {
        static::assertSame(
            ["\x00\x02\x01\x00\x00\x00\x00\x00\x00\x00"],
            (new KeyEncoder(
                ['k', 'id'],
                declared: schema(int_schema('k', nullable: true), int_schema('id')),
            ))->of(array_to_rows([['id' => 1]], schema(int_schema('id')))),
        );
    }
}

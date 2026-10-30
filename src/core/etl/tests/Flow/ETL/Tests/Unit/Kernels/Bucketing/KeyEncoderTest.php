<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Kernels\Bucketing;

use DateTimeImmutable;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Kernels\Bucketing\KeyEncoder;
use Flow\ETL\Tests\FlowTestCase;

use function array_merge;
use function array_unique;
use function Flow\ETL\DSL\array_to_rows;
use function Flow\ETL\DSL\bool_schema;
use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

use const NAN;

final class KeyEncoderTest extends FlowTestCase
{
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
}

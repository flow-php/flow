<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateTimeImmutable;
use Flow\ETL\Functions\Comparison;
use Flow\ETL\Functions\Contains;
use Flow\ETL\Functions\EndsWith;
use Flow\ETL\Functions\IsIn;
use Flow\ETL\Functions\IsNotNull;
use Flow\ETL\Functions\IsNotNumeric;
use Flow\ETL\Functions\IsNull;
use Flow\ETL\Functions\IsNumeric;
use Flow\ETL\Functions\IsType;
use Flow\ETL\Functions\StartsWith;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Type\Comparison\Operator;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;

final class BinaryComparisonsTest extends FlowTestCase
{
    public function test_equals(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::EQUAL, ref('b')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
            ],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), datetime_schema('d'), datetime_schema('e')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('d'), Operator::EQUAL, ref('e')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
            ],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), datetime_schema('d'), datetime_schema('e')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::EQUAL, ref('c')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
            ],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), datetime_schema('d'), datetime_schema('e')),
        ));
    }

    public function test_greater_than(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN, ref('c')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN, ref('f')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('f'), Operator::GREATER_THAN, ref('c')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('f'), Operator::GREATER_THAN, ref('f')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN, ref('b')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN_EQUAL, ref('c')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN_EQUAL, ref('b')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('e'), Operator::GREATER_THAN_EQUAL, ref('d')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(
                ref('e'),
                Operator::GREATER_THAN_EQUAL,
                lit(new DateTimeImmutable('2022-01-01 00:00:00 UTC')),
            ),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(
                ref('e'),
                Operator::GREATER_THAN_EQUAL,
                lit(new DateTimeImmutable('2024-01-01 00:00:00 UTC')),
            ),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN_EQUAL, ref('f')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('f'), Operator::GREATER_THAN_EQUAL, ref('c')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('f'), Operator::GREATER_THAN_EQUAL, ref('f')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-02 00:00:00 UTC'),
                'f' => null,
            ],
            schema(
                int_schema('a'),
                int_schema('b'),
                int_schema('c'),
                datetime_schema('d'),
                datetime_schema('e'),
                str_schema('f', nullable: true),
            ),
        ));
    }

    public function test_greater_than_equal_with_null_in_strict_mode(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN_EQUAL, ref('f')),
            ['a' => 100, 'f' => null],
            schema(int_schema('a'), str_schema('f', nullable: true)),
        ));
    }

    public function test_greater_than_with_null_in_strict_mode(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::GREATER_THAN, ref('f')),
            ['a' => 100, 'f' => null],
            schema(int_schema('a'), str_schema('f', nullable: true)),
        ));
    }

    public function test_is_in(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsIn(ref('a'), lit(1)),
            [
                'a' => [1, 2, 3, 4, 5],
                'b' => ['a', 'b', 'c'],
                'c' => 'another',
                'd' => 4,
                'e' => 'b',
            ],
            schema(
                list_schema('a', type_list(type_integer())),
                list_schema('b', type_list(type_string())),
                str_schema('c'),
                int_schema('d'),
                str_schema('e'),
            ),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new IsIn(ref('a'), lit(10)),
            [
                'a' => [1, 2, 3, 4, 5],
                'b' => ['a', 'b', 'c'],
                'c' => 'another',
                'd' => 4,
                'e' => 'b',
            ],
            schema(
                list_schema('a', type_list(type_integer())),
                list_schema('b', type_list(type_string())),
                str_schema('c'),
                int_schema('d'),
                str_schema('e'),
            ),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsIn(ref('a'), ref('d')),
            [
                'a' => [1, 2, 3, 4, 5],
                'b' => ['a', 'b', 'c'],
                'c' => 'another',
                'd' => 4,
                'e' => 'b',
            ],
            schema(
                list_schema('a', type_list(type_integer())),
                list_schema('b', type_list(type_string())),
                str_schema('c'),
                int_schema('d'),
                str_schema('e'),
            ),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsIn(ref('b'), ref('e')),
            [
                'a' => [1, 2, 3, 4, 5],
                'b' => ['a', 'b', 'c'],
                'c' => 'another',
                'd' => 4,
                'e' => 'b',
            ],
            schema(
                list_schema('a', type_list(type_integer())),
                list_schema('b', type_list(type_string())),
                str_schema('c'),
                int_schema('d'),
                str_schema('e'),
            ),
        ));
    }

    public function test_is_in_with_null_array_in_strict_mode(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new IsIn(ref('a'), ref('d')),
            ['a' => null, 'd' => 1],
            schema(str_schema('a', nullable: true), int_schema('d')),
        ));
    }

    public function test_is_numeric(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsNumeric(ref('a')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new IsNumeric(ref('b')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new IsNotNumeric(ref('a')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new IsNotNumeric(ref('b')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new IsNotNumeric(lit(null)),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsNumeric(lit(1000)),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
    }

    public function test_is_type(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsType(ref('a'), 'integer', 'string'),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new IsType(ref('a'), type_string()),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
    }

    public function test_is_type_with_non_existing_type_class(): void
    {
        $this->expectExceptionMessage('Unknown type \'aaa\'');

        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new IsType(ref('a'), 'aaa'),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
    }

    public function test_less_than(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN, ref('c')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN, ref('d')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('d'), Operator::LESS_THAN, ref('d')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('d'), Operator::LESS_THAN, ref('c')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN, ref('b')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('c'), Operator::LESS_THAN_EQUAL, ref('a')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN_EQUAL, ref('b')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN_EQUAL, ref('d')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('d'), Operator::LESS_THAN_EQUAL, ref('c')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('d'), Operator::LESS_THAN_EQUAL, ref('d')),
            ['a' => 100, 'b' => 100, 'c' => 10, 'd' => null],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), str_schema('d', nullable: true)),
        ));
    }

    public function test_less_than_equal_with_null_in_strict_mode(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN_EQUAL, ref('d')),
            ['a' => 100, 'd' => null],
            schema(int_schema('a'), str_schema('d', nullable: true)),
        ));
    }

    public function test_less_than_with_null_in_strict_mode(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::LESS_THAN, ref('d')),
            ['a' => 100, 'd' => null],
            schema(int_schema('a'), str_schema('d', nullable: true)),
        ));
    }

    public function test_not_equals(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::NOT_EQUAL, ref('b')),
            ['a' => 100, 'b' => 100, 'c' => 10],
            schema(int_schema('a'), int_schema('b'), int_schema('c')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::NOT_EQUAL, ref('c')),
            ['a' => 100, 'b' => 100, 'c' => 10],
            schema(int_schema('a'), int_schema('b'), int_schema('c')),
        ));
    }

    public function test_not_same(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::NOT_IDENTICAL, ref('c')),
            ['a' => 100, 'b' => 100, 'c' => 10],
            schema(int_schema('a'), int_schema('b'), int_schema('c')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::NOT_IDENTICAL, ref('b')),
            ['a' => 100, 'b' => 100, 'c' => 10],
            schema(int_schema('a'), int_schema('b'), int_schema('c')),
        ));
    }

    public function test_null(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new IsNull(ref('a')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsNull(ref('b')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsNotNull(ref('a')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new IsNotNull(ref('b')),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsNull(lit(null)),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new IsNotNull(lit(1000)),
            ['a' => 100, 'b' => null],
            schema(int_schema('a'), str_schema('b', nullable: true)),
        ));
    }

    public function test_same(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::IDENTICAL, ref('b')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
            ],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), datetime_schema('d'), datetime_schema('e')),
        ));
        // two datetimes of the same instant are the same value
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('d'), Operator::IDENTICAL, ref('e')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
            ],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), datetime_schema('d'), datetime_schema('e')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Comparison(ref('a'), Operator::IDENTICAL, ref('c')),
            [
                'a' => 100,
                'b' => 100,
                'c' => 10,
                'd' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
                'e' => type_datetime()->cast('2023-01-01 00:00:00 UTC'),
            ],
            schema(int_schema('a'), int_schema('b'), int_schema('c'), datetime_schema('d'), datetime_schema('e')),
        ));
    }

    public function test_starts_ends_with(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new StartsWith(ref('a'), lit('some not')),
            [
                'a' => 'some not too long string',
                'b' => 'another not too long text',
                'c' => 'another',
                'd' => 'text',
            ],
            schema(str_schema('a'), str_schema('b'), str_schema('c'), str_schema('d')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new EndsWith(ref('a'), lit('long string')),
            [
                'a' => 'some not too long string',
                'b' => 'another not too long text',
                'c' => 'another',
                'd' => 'text',
            ],
            schema(str_schema('a'), str_schema('b'), str_schema('c'), str_schema('d')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new StartsWith(ref('b'), ref('c')),
            [
                'a' => 'some not too long string',
                'b' => 'another not too long text',
                'c' => 'another',
                'd' => 'text',
            ],
            schema(str_schema('a'), str_schema('b'), str_schema('c'), str_schema('d')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new EndsWith(ref('b'), ref('d')),
            [
                'a' => 'some not too long string',
                'b' => 'another not too long text',
                'c' => 'another',
                'd' => 'text',
            ],
            schema(str_schema('a'), str_schema('b'), str_schema('c'), str_schema('d')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            new Contains(ref('a'), lit('too long')),
            [
                'a' => 'some not too long string',
                'b' => 'another not too long text',
                'c' => 'another',
                'd' => 'text',
            ],
            schema(str_schema('a'), str_schema('b'), str_schema('c'), str_schema('d')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            new Contains(ref('a'), lit('blablabla')),
            [
                'a' => 'some not too long string',
                'b' => 'another not too long text',
                'c' => 'another',
                'd' => 'text',
            ],
            schema(str_schema('a'), str_schema('b'), str_schema('c'), str_schema('d')),
        ));
    }
}

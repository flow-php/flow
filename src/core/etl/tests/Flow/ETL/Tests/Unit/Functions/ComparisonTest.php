<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException as EtlInvalidArgumentException;
use Flow\ETL\Functions\Comparison;
use Flow\ETL\Functions\ReferenceResolver;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Fixtures\Enum\BackedStringEnum;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;
use Flow\Types\Type;
use Flow\Types\Type\Comparison\Operator;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;

use function Flow\ETL\DSL\date_schema;
use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\definition_from_type;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\Types\DSL\type_date;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_enum;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_json;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_numeric_string;
use function Flow\Types\DSL\type_time;
use function Flow\Types\DSL\type_uuid;
use function Flow\Types\DSL\type_xml_element;

use const NAN;

final class ComparisonTest extends FlowTestCase
{
    #[TestWith([Operator::DIFFERENT])]
    #[TestWith([Operator::SPACE_SHIP])]
    public function test_refuses_an_operator_that_does_not_compare_two_values(Operator $operator): void
    {
        $this->expectException(EtlInvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Comparison does not support "' . $operator->value . '", use one of: ==, !=, ===, !==, >, >=, <, <=',
        );

        new Comparison(ref('a'), $operator, ref('b'));
    }

    public function test_equality_casts_a_temporal_string(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('d')->equals(lit('2024-01-01')),
            ['d' => new DateTimeImmutable('2024-01-01')],
            schema(date_schema('d')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('d')->notEquals(lit('2024-01-01')),
            ['d' => new DateTimeImmutable('2024-01-01')],
            schema(date_schema('d')),
        ));
    }

    #[TestWith([Operator::IDENTICAL])]
    #[TestWith([Operator::NOT_IDENTICAL])]
    public function test_identity_refuses_a_temporal_string(Operator $operator): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            "Can't compare '(date "
            . $operator->value
            . " string)' due to data type mismatch - an explicit cast is required.",
        );

        (new ReferenceResolver())
            ->resolve(new Comparison(ref('d'), $operator, lit('2024-01-01')), schema(date_schema('d')))
            ->returns();
    }

    public function test_a_cast_error_names_the_column_read_never_the_alias(): void
    {
        $this->expectException(EvaluationException::class);
        $this->expectExceptionMessage("Column \"a\" value 'x' cannot be cast to integer to compare");

        (new FunctionContext(flow_context()))->eval(
            ref('a')->as('b')->equals(lit(1)),
            ['a' => 'x'],
            schema(str_schema('a')),
        );
    }

    public function test_negations_keep_null_unknown_and_identity_defined(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('a')->notEquals(lit(1)),
            ['a' => null],
            schema(int_schema('a', nullable: true)),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->notSame(lit(1)),
            ['a' => null],
            schema(int_schema('a', nullable: true)),
        ));
    }

    public function test_null_operand_yields_null(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(1)),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(ref('b')),
            ['a' => null, 'b' => null],
            schema(str_schema('a', nullable: true), str_schema('b', nullable: true)),
        ));
    }

    public function test_equal_values(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(1)),
            ['a' => 1],
            schema(int_schema('a')),
        ));
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit('x')),
            ['a' => 'x'],
            schema(str_schema('a')),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(2)),
            ['a' => 1],
            schema(int_schema('a')),
        ));
    }

    /**
     * @return Generator<string, array{Type<mixed>, mixed, mixed, bool}>
     */
    public static function values_of_one_type(): Generator
    {
        yield 'uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000001',
            true,
        ];
        yield 'other uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000002',
            false,
        ];
        yield 'json' => [type_json(), '{"a":1,"b":2}', '{"a":1,"b":2}', true];
        yield 'json compares its stored text' => [type_json(), '{"a":1,"b":2}', '{"b":2,"a":1}', false];
        yield 'enum' => [type_enum(BackedStringEnum::class), BackedStringEnum::one, BackedStringEnum::one, true];
        yield 'datetime of the same instant in another zone' => [
            type_datetime(),
            new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('Europe/Warsaw')),
            true,
        ];
        yield 'date' => [type_date(), new DateTimeImmutable('2024-01-01'), new DateTimeImmutable('2024-01-01'), true];
        yield 'time' => [type_time(), new DateInterval('PT60M'), new DateInterval('PT1H'), true];
        yield 'xml elements of different documents' => [
            type_xml_element(),
            type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild,
            type_xml_element()->cast('<section><p>a</p></section>')->firstElementChild,
            true,
        ];
        yield 'other xml elements' => [
            type_xml_element(),
            type_xml_element()->cast('<div><p>a</p></div>')->firstElementChild,
            type_xml_element()->cast('<div><p>b</p></div>')->firstElementChild,
            false,
        ];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('values_of_one_type')]
    public function test_values_of_one_type_compare_by_value(Type $type, mixed $left, mixed $right, bool $equal): void
    {
        static::assertSame($equal, (new FunctionContext(flow_context()))->eval(
            ref('a')->equals(ref('b')),
            ['a' => $left, 'b' => $right],
            schema(definition_from_type('a', $type), definition_from_type('b', $type)),
        ));
    }

    public function test_a_datetime_equals_a_literal_of_the_same_instant_in_another_zone(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->equals(lit(new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('Europe/Warsaw')))),
            ['a' => new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC'))],
            schema(definition_from_type('a', type_datetime())),
        ));
    }

    public function test_a_uuid_is_not_comparable_with_a_string(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage("Can't compare '(uuid == string)' due to data type mismatch");

        (new ReferenceResolver())
            ->resolve(
                ref('u')->equals(lit('00000000-0000-4000-8000-000000000001')),
                schema(definition_from_type('u', type_uuid())),
            )
            ->returns();
    }

    public function test_null_is_identical_to_null(): void
    {
        // PHP identity, not SQL equality - ->same() is the migration path for both-null checks.
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->same(lit(null)),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('a')->same(lit(1)),
            ['a' => null],
            schema(str_schema('a', nullable: true)),
        ));
    }

    /**
     * @return Generator<string, array{Type<mixed>, mixed, mixed, bool}>
     */
    public static function identical_values_of_one_type(): Generator
    {
        yield 'uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000001',
            true,
        ];
        yield 'other uuid' => [
            type_uuid(),
            '00000000-0000-4000-8000-000000000001',
            '00000000-0000-4000-8000-000000000002',
            false,
        ];
        yield 'json' => [type_json(), '{"a":1,"b":2}', '{"a":1,"b":2}', true];
        yield 'json compares its stored text' => [type_json(), '{"a":1,"b":2}', '{"b":2,"a":1}', false];
        yield 'enum' => [type_enum(BackedStringEnum::class), BackedStringEnum::one, BackedStringEnum::one, true];
        yield 'datetime of the same instant in another zone' => [
            type_datetime(),
            new DateTimeImmutable('2024-01-01 12:00:00', new DateTimeZone('UTC')),
            new DateTimeImmutable('2024-01-01 13:00:00', new DateTimeZone('Europe/Warsaw')),
            true,
        ];
        yield 'date' => [type_date(), new DateTimeImmutable('2024-01-01'), new DateTimeImmutable('2024-01-01'), true];
        yield 'time' => [type_time(), new DateInterval('PT60M'), new DateInterval('PT1H'), true];
    }

    /**
     * @param Type<mixed> $type
     */
    #[DataProvider('identical_values_of_one_type')]
    public function test_identical_values_of_one_type_are_the_same_by_value(
        Type $type,
        mixed $left,
        mixed $right,
        bool $same,
    ): void {
        static::assertSame($same, (new FunctionContext(flow_context()))->eval(
            ref('a')->same(ref('b')),
            ['a' => $left, 'b' => $right],
            schema(definition_from_type('a', $type), definition_from_type('b', $type)),
        ));
    }

    public function test_an_integer_column_compared_to_a_string_literal_still_works(): void
    {
        // Comparator::comparable(integer, string) stays deliberately loose - 5 > '3' keeps working.
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->greaterThan(lit('3')),
            ['a' => 5],
            schema(int_schema('a')),
        ));
    }

    public function test_greater_than_arrays(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit([1, 0])),
            ['v' => [1, 9]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit([1, 9])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit([1, 0])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
    }

    public function test_greater_than_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-06-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2023-06-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
    }

    public function test_greater_than_floats(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(1.5)),
            ['v' => 2.5],
            schema(float_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(2.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(1.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
    }

    public function test_greater_than_integers(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(10)),
            ['v' => 20],
            schema(int_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(20)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(10)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
    }

    public function test_greater_than_returns_null_for_null_left(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThan(lit(10)),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_returns_null_for_null_right(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThan(ref('other')),
            ['v' => 10, 'other' => null],
            schema(int_schema('v'), str_schema('other', nullable: true)),
        ));
    }

    public function test_greater_than_returns_null_for_null_string(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThan(lit('a')),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_returns_null_for_null_datetime(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThan(lit(new DateTimeImmutable('2024-01-01'))),
            ['v' => null],
            schema(datetime_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_returns_null_for_null_time_interval(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThan(lit(new DateInterval('PT1H'))),
            ['v' => null],
            schema(time_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_returns_null_for_null_array(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThan(lit([1, 0])),
            ['v' => null],
            schema(list_schema('v', type_list(type_integer()), nullable: true)),
        ));
    }

    public function test_greater_than_strings(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit('apple')),
            ['v' => 'banana'],
            schema(str_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit('banana')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit('apple')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
    }

    public function test_greater_than_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        lit(new DateTimeImmutable('now'))->greaterThan(lit(5))->returns();
    }

    public function test_greater_than_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT5H')],
            schema(time_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(new DateInterval('PT5H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThan(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
    }

    public function test_greater_than_orders_strings_by_bytes(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->greaterThan(lit('10')),
            ['a' => '9'],
            schema(str_schema('a')),
        ));
    }

    public function test_greater_than_orders_a_numeric_string_column_numerically(): void
    {
        $context = flow_context();

        static::assertFalse((new FunctionContext($context))->eval(
            ref('a')->greaterThan(lit('10')),
            ['a' => '9'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('a')->greaterThan(lit('18446744073709551614')),
            ['a' => '18446744073709551615'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
    }

    public function test_greater_than_ranks_nan_above_every_number(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('a')->greaterThan(lit(1.0)),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            lit(1.0)->greaterThan(ref('a')),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
    }

    public function test_greater_than_equal_arrays(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit([1, 0])),
            ['v' => [1, 9]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit([1, 0])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit([1, 9])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
    }

    public function test_greater_than_equal_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-06-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(new DateTimeImmutable('2024-06-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
    }

    public function test_greater_than_equal_floats(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(1.5)),
            ['v' => 2.5],
            schema(float_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(1.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(2.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
    }

    public function test_greater_than_equal_integers(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(10)),
            ['v' => 20],
            schema(int_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(10)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(20)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
    }

    public function test_greater_than_equal_returns_null_for_null_array(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThanEqual(lit([1, 0])),
            ['v' => null],
            schema(list_schema('v', type_list(type_integer()), nullable: true)),
        ));
    }

    public function test_greater_than_equal_returns_null_for_null_datetime(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            ['v' => null],
            schema(datetime_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_equal_returns_null_for_null_left(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThanEqual(lit(10)),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_equal_returns_null_for_null_right(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThanEqual(ref('other')),
            ['v' => 10, 'other' => null],
            schema(int_schema('v'), str_schema('other', nullable: true)),
        ));
    }

    public function test_greater_than_equal_returns_null_for_null_string(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThanEqual(lit('a')),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_equal_returns_null_for_null_time_interval(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->greaterThanEqual(lit(new DateInterval('PT1H'))),
            ['v' => null],
            schema(time_schema('v', nullable: true)),
        ));
    }

    public function test_greater_than_equal_strings(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit('apple')),
            ['v' => 'banana'],
            schema(str_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit('apple')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit('banana')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
    }

    public function test_greater_than_equal_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        lit(new DateTimeImmutable('now'))->greaterThanEqual(lit(5))->returns();
    }

    public function test_greater_than_equal_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT5H')],
            schema(time_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->greaterThanEqual(lit(new DateInterval('PT5H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
    }

    public function test_greater_than_equal_orders_strings_by_bytes(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            ref('a')->greaterThanEqual(lit('10')),
            ['a' => '9'],
            schema(str_schema('a')),
        ));
    }

    public function test_greater_than_equal_orders_a_numeric_string_column_numerically(): void
    {
        $context = flow_context();

        static::assertFalse((new FunctionContext($context))->eval(
            ref('a')->greaterThanEqual(lit('10')),
            ['a' => '9'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('a')->greaterThanEqual(lit('18446744073709551614')),
            ['a' => '18446744073709551615'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
    }

    public function test_greater_than_equal_ranks_nan_above_every_number(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('a')->greaterThanEqual(lit(1.0)),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            lit(1.0)->greaterThanEqual(ref('a')),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
    }

    public function test_less_than_arrays(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit([1, 9])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit([1, 0])),
            ['v' => [1, 9]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit([1, 0])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
    }

    public function test_less_than_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(new DateTimeImmutable('2024-06-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-06-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
    }

    public function test_less_than_floats(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(2.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(1.5)),
            ['v' => 2.5],
            schema(float_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(1.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
    }

    public function test_less_than_integers(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(20)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(10)),
            ['v' => 20],
            schema(int_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(10)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
    }

    public function test_less_than_returns_null_for_null_array(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThan(lit([1, 0])),
            ['v' => null],
            schema(list_schema('v', type_list(type_integer()), nullable: true)),
        ));
    }

    public function test_less_than_returns_null_for_null_datetime(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThan(lit(new DateTimeImmutable('2024-01-01'))),
            ['v' => null],
            schema(datetime_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_returns_null_for_null_left(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThan(lit(10)),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_returns_null_for_null_right(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThan(ref('other')),
            ['v' => 10, 'other' => null],
            schema(int_schema('v'), str_schema('other', nullable: true)),
        ));
    }

    public function test_less_than_returns_null_for_null_string(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThan(lit('a')),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_returns_null_for_null_time_interval(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThan(lit(new DateInterval('PT1H'))),
            ['v' => null],
            schema(time_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_strings(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit('banana')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit('apple')),
            ['v' => 'banana'],
            schema(str_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit('apple')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
    }

    public function test_less_than_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        lit(new DateTimeImmutable('now'))->lessThan(lit(5))->returns();
    }

    public function test_less_than_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(new DateInterval('PT5H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT5H')],
            schema(time_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThan(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
    }

    public function test_less_than_orders_strings_by_bytes(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('a')->lessThan(lit('10')),
            ['a' => '9'],
            schema(str_schema('a')),
        ));
    }

    public function test_less_than_orders_a_numeric_string_column_numerically(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('a')->lessThan(lit('10')),
            ['a' => '9'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('a')->lessThan(lit('18446744073709551614')),
            ['a' => '18446744073709551615'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
    }

    public function test_less_than_ranks_nan_above_every_number(): void
    {
        $context = flow_context();

        static::assertFalse((new FunctionContext($context))->eval(
            ref('a')->lessThan(lit(1.0)),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            lit(1.0)->lessThan(ref('a')),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
    }

    public function test_less_than_equal_arrays(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit([1, 9])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit([1, 0])),
            ['v' => [1, 0]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit([1, 0])),
            ['v' => [1, 9]],
            schema(list_schema('v', type_list(type_integer()))),
        ));
    }

    public function test_less_than_equal_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(new DateTimeImmutable('2024-06-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-01-01'),
            ],
            schema(datetime_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            [
                'v' => new DateTimeImmutable('2024-06-01'),
            ],
            schema(datetime_schema('v')),
        ));
    }

    public function test_less_than_equal_floats(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(2.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(1.5)),
            ['v' => 1.5],
            schema(float_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(1.5)),
            ['v' => 2.5],
            schema(float_schema('v')),
        ));
    }

    public function test_less_than_equal_integers(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(20)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(10)),
            ['v' => 10],
            schema(int_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(10)),
            ['v' => 20],
            schema(int_schema('v')),
        ));
    }

    public function test_less_than_equal_returns_null_for_null_array(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThanEqual(lit([1, 0])),
            ['v' => null],
            schema(list_schema('v', type_list(type_integer()), nullable: true)),
        ));
    }

    public function test_less_than_equal_returns_null_for_null_datetime(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThanEqual(lit(new DateTimeImmutable('2024-01-01'))),
            ['v' => null],
            schema(datetime_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_equal_returns_null_for_null_left(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThanEqual(lit(10)),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_equal_returns_null_for_null_right(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThanEqual(ref('other')),
            ['v' => 10, 'other' => null],
            schema(int_schema('v'), str_schema('other', nullable: true)),
        ));
    }

    public function test_less_than_equal_returns_null_for_null_string(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThanEqual(lit('a')),
            ['v' => null],
            schema(str_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_equal_returns_null_for_null_time_interval(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            ref('v')->lessThanEqual(lit(new DateInterval('PT1H'))),
            ['v' => null],
            schema(time_schema('v', nullable: true)),
        ));
    }

    public function test_less_than_equal_strings(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit('banana')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit('apple')),
            ['v' => 'apple'],
            schema(str_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit('apple')),
            ['v' => 'banana'],
            schema(str_schema('v')),
        ));
    }

    public function test_less_than_equal_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        lit(new DateTimeImmutable('now'))->lessThanEqual(lit(5))->returns();
    }

    public function test_less_than_equal_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(new DateInterval('PT5H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT1H')],
            schema(time_schema('v')),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('v')->lessThanEqual(lit(new DateInterval('PT1H'))),
            ['v' => new DateInterval('PT5H')],
            schema(time_schema('v')),
        ));
    }

    public function test_less_than_equal_orders_strings_by_bytes(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            ref('a')->lessThanEqual(lit('10')),
            ['a' => '9'],
            schema(str_schema('a')),
        ));
    }

    public function test_less_than_equal_orders_a_numeric_string_column_numerically(): void
    {
        $context = flow_context();

        static::assertTrue((new FunctionContext($context))->eval(
            ref('a')->lessThanEqual(lit('10')),
            ['a' => '9'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
        static::assertFalse((new FunctionContext($context))->eval(
            ref('a')->lessThanEqual(lit('18446744073709551614')),
            ['a' => '18446744073709551615'],
            schema(definition_from_type('a', type_numeric_string())),
        ));
    }

    public function test_less_than_equal_ranks_nan_above_every_number(): void
    {
        $context = flow_context();

        static::assertFalse((new FunctionContext($context))->eval(
            ref('a')->lessThanEqual(lit(1.0)),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
        static::assertTrue((new FunctionContext($context))->eval(
            lit(1.0)->lessThanEqual(ref('a')),
            ['a' => NAN],
            schema(float_schema('a')),
        ));
    }
}

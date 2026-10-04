<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;

use function Flow\ETL\DSL\datetime_schema;
use function Flow\ETL\DSL\float_schema;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\list_schema;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\ETL\DSL\time_schema;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class LessThanTest extends FlowTestCase
{
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
}

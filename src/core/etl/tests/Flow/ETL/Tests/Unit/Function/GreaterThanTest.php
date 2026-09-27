<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;

use function Flow\ETL\DSL\array_to_row;
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

final class GreaterThanTest extends FlowTestCase
{
    public function test_an_integer_column_compared_to_a_string_literal_still_works(): void
    {
        // Comparator::comparable(integer, string) stays deliberately loose - 5 > '3' keeps working.
        static::assertTrue(
            ref('a')->greaterThan(lit('3'))->eval(array_to_row(['a' => 5], schema(int_schema('a'))), flow_context()),
        );
    }

    public function test_greater_than_arrays(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThan(lit([1, 0]))
                ->eval(array_to_row(['v' => [1, 9]], schema(list_schema('v', type_list(type_integer())))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit([1, 9]))
                ->eval(array_to_row(['v' => [1, 0]], schema(list_schema('v', type_list(type_integer())))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit([1, 0]))
                ->eval(array_to_row(['v' => [1, 0]], schema(list_schema('v', type_list(type_integer())))), $context),
        );
    }

    public function test_greater_than_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(array_to_row([
                    'v' => new DateTimeImmutable('2024-06-01'),
                ], schema(datetime_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(array_to_row([
                    'v' => new DateTimeImmutable('2023-06-01'),
                ], schema(datetime_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(array_to_row([
                    'v' => new DateTimeImmutable('2024-01-01'),
                ], schema(datetime_schema('v'))), $context),
        );
    }

    public function test_greater_than_floats(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')->greaterThan(lit(1.5))->eval(array_to_row(['v' => 2.5], schema(float_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')->greaterThan(lit(2.5))->eval(array_to_row(['v' => 1.5], schema(float_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')->greaterThan(lit(1.5))->eval(array_to_row(['v' => 1.5], schema(float_schema('v'))), $context),
        );
    }

    public function test_greater_than_integers(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')->greaterThan(lit(10))->eval(array_to_row(['v' => 20], schema(int_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')->greaterThan(lit(20))->eval(array_to_row(['v' => 10], schema(int_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')->greaterThan(lit(10))->eval(array_to_row(['v' => 10], schema(int_schema('v'))), $context),
        );
    }

    public function test_greater_than_returns_null_for_null_left(): void
    {
        static::assertNull(
            ref('v')
                ->greaterThan(lit(10))
                ->eval(array_to_row(['v' => null], schema(str_schema('v', nullable: true))), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_right(): void
    {
        static::assertNull(
            ref('v')
                ->greaterThan(ref('other'))
                ->eval(
                    array_to_row(
                        ['v' => 10, 'other' => null],
                        schema(int_schema('v'), str_schema('other', nullable: true)),
                    ),
                    flow_context(),
                ),
        );
    }

    public function test_greater_than_returns_null_for_null_string(): void
    {
        static::assertNull(
            ref('v')
                ->greaterThan(lit('a'))
                ->eval(array_to_row(['v' => null], schema(str_schema('v', nullable: true))), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_datetime(): void
    {
        static::assertNull(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(array_to_row(['v' => null], schema(str_schema('v', nullable: true))), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_time_interval(): void
    {
        static::assertNull(
            ref('v')
                ->greaterThan(lit(new DateInterval('PT1H')))
                ->eval(array_to_row(['v' => null], schema(str_schema('v', nullable: true))), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_array(): void
    {
        static::assertNull(
            ref('v')
                ->greaterThan(lit([1, 0]))
                ->eval(array_to_row(['v' => null], schema(str_schema('v', nullable: true))), flow_context()),
        );
    }

    public function test_greater_than_strings(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThan(lit('apple'))
                ->eval(array_to_row(['v' => 'banana'], schema(str_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit('banana'))
                ->eval(array_to_row(['v' => 'apple'], schema(str_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit('apple'))
                ->eval(array_to_row(['v' => 'apple'], schema(str_schema('v'))), $context),
        );
    }

    public function test_greater_than_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        lit(new DateTimeImmutable('now'))->greaterThan(lit(5))->returns();
    }

    public function test_greater_than_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThan(lit(new DateInterval('PT1H')))
                ->eval(array_to_row(['v' => new DateInterval('PT5H')], schema(time_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateInterval('PT5H')))
                ->eval(array_to_row(['v' => new DateInterval('PT1H')], schema(time_schema('v'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateInterval('PT1H')))
                ->eval(array_to_row(['v' => new DateInterval('PT1H')], schema(time_schema('v'))), $context),
        );
    }
}

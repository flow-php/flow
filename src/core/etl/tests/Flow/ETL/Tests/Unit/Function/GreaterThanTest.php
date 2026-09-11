<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class GreaterThanTest extends FlowTestCase
{
    public function test_an_integer_column_compared_to_a_string_literal_still_works(): void
    {
        // Comparator::comparable(integer, string) stays deliberately loose - 5 > '3' keeps working.
        static::assertTrue(ref('a')->greaterThan(lit('3'))->eval(row(['a' => 5]), flow_context()));
    }

    public function test_greater_than_arrays(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThan(lit([1, 0]))->eval(row(['v' => [1, 9]]), $context));
        static::assertFalse(ref('v')->greaterThan(lit([1, 9]))->eval(row(['v' => [1, 0]]), $context));
        static::assertFalse(ref('v')->greaterThan(lit([1, 0]))->eval(row(['v' => [1, 0]]), $context));
    }

    public function test_greater_than_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(['v' => new DateTimeImmutable('2024-06-01')]), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(['v' => new DateTimeImmutable('2023-06-01')]), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(['v' => new DateTimeImmutable('2024-01-01')]), $context),
        );
    }

    public function test_greater_than_floats(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThan(lit(1.5))->eval(row(['v' => 2.5]), $context));
        static::assertFalse(ref('v')->greaterThan(lit(2.5))->eval(row(['v' => 1.5]), $context));
        static::assertFalse(ref('v')->greaterThan(lit(1.5))->eval(row(['v' => 1.5]), $context));
    }

    public function test_greater_than_integers(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThan(lit(10))->eval(row(['v' => 20]), $context));
        static::assertFalse(ref('v')->greaterThan(lit(20))->eval(row(['v' => 10]), $context));
        static::assertFalse(ref('v')->greaterThan(lit(10))->eval(row(['v' => 10]), $context));
    }

    public function test_greater_than_returns_null_for_null_left(): void
    {
        static::assertNull(ref('v')->greaterThan(lit(10))->eval(row(['v' => null]), flow_context()));
    }

    public function test_greater_than_returns_null_for_null_right(): void
    {
        static::assertNull(
            ref('v')->greaterThan(ref('other'))->eval(row(['v' => 10, 'other' => null]), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_string(): void
    {
        static::assertNull(ref('v')->greaterThan(lit('a'))->eval(row(['v' => null]), flow_context()));
    }

    public function test_greater_than_returns_null_for_null_datetime(): void
    {
        static::assertNull(
            ref('v')->greaterThan(lit(new DateTimeImmutable('2024-01-01')))->eval(row(['v' => null]), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_time_interval(): void
    {
        static::assertNull(
            ref('v')->greaterThan(lit(new DateInterval('PT1H')))->eval(row(['v' => null]), flow_context()),
        );
    }

    public function test_greater_than_returns_null_for_null_array(): void
    {
        static::assertNull(ref('v')->greaterThan(lit([1, 0]))->eval(row(['v' => null]), flow_context()));
    }

    public function test_greater_than_strings(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThan(lit('apple'))->eval(row(['v' => 'banana']), $context));
        static::assertFalse(ref('v')->greaterThan(lit('banana'))->eval(row(['v' => 'apple']), $context));
        static::assertFalse(ref('v')->greaterThan(lit('apple'))->eval(row(['v' => 'apple']), $context));
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
                ->eval(row(['v' => new DateInterval('PT5H')]), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateInterval('PT5H')))
                ->eval(row(['v' => new DateInterval('PT1H')]), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThan(lit(new DateInterval('PT1H')))
                ->eval(row(['v' => new DateInterval('PT1H')]), $context),
        );
    }
}

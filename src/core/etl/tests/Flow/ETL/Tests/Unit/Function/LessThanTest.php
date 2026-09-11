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

final class LessThanTest extends FlowTestCase
{
    public function test_less_than_arrays(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->lessThan(lit([1, 9]))->eval(row(['v' => [1, 0]]), $context));
        static::assertFalse(ref('v')->lessThan(lit([1, 0]))->eval(row(['v' => [1, 9]]), $context));
        static::assertFalse(ref('v')->lessThan(lit([1, 0]))->eval(row(['v' => [1, 0]]), $context));
    }

    public function test_less_than_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->lessThan(lit(new DateTimeImmutable('2024-06-01')))
                ->eval(row(['v' => new DateTimeImmutable('2024-01-01')]), $context),
        );
        static::assertFalse(
            ref('v')
                ->lessThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(['v' => new DateTimeImmutable('2024-06-01')]), $context),
        );
        static::assertFalse(
            ref('v')
                ->lessThan(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(['v' => new DateTimeImmutable('2024-01-01')]), $context),
        );
    }

    public function test_less_than_floats(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->lessThan(lit(2.5))->eval(row(['v' => 1.5]), $context));
        static::assertFalse(ref('v')->lessThan(lit(1.5))->eval(row(['v' => 2.5]), $context));
        static::assertFalse(ref('v')->lessThan(lit(1.5))->eval(row(['v' => 1.5]), $context));
    }

    public function test_less_than_integers(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->lessThan(lit(20))->eval(row(['v' => 10]), $context));
        static::assertFalse(ref('v')->lessThan(lit(10))->eval(row(['v' => 20]), $context));
        static::assertFalse(ref('v')->lessThan(lit(10))->eval(row(['v' => 10]), $context));
    }

    public function test_less_than_returns_null_for_null_array(): void
    {
        static::assertNull(ref('v')->lessThan(lit([1, 0]))->eval(row(['v' => null]), flow_context()));
    }

    public function test_less_than_returns_null_for_null_datetime(): void
    {
        static::assertNull(
            ref('v')->lessThan(lit(new DateTimeImmutable('2024-01-01')))->eval(row(['v' => null]), flow_context()),
        );
    }

    public function test_less_than_returns_null_for_null_left(): void
    {
        static::assertNull(ref('v')->lessThan(lit(10))->eval(row(['v' => null]), flow_context()));
    }

    public function test_less_than_returns_null_for_null_right(): void
    {
        static::assertNull(ref('v')->lessThan(ref('other'))->eval(row(['v' => 10, 'other' => null]), flow_context()));
    }

    public function test_less_than_returns_null_for_null_string(): void
    {
        static::assertNull(ref('v')->lessThan(lit('a'))->eval(row(['v' => null]), flow_context()));
    }

    public function test_less_than_returns_null_for_null_time_interval(): void
    {
        static::assertNull(ref('v')->lessThan(lit(new DateInterval('PT1H')))->eval(row(['v' => null]), flow_context()));
    }

    public function test_less_than_strings(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->lessThan(lit('banana'))->eval(row(['v' => 'apple']), $context));
        static::assertFalse(ref('v')->lessThan(lit('apple'))->eval(row(['v' => 'banana']), $context));
        static::assertFalse(ref('v')->lessThan(lit('apple'))->eval(row(['v' => 'apple']), $context));
    }

    public function test_less_than_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        lit(new DateTimeImmutable('now'))->lessThan(lit(5))->returns();
    }

    public function test_less_than_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')->lessThan(lit(new DateInterval('PT5H')))->eval(row(['v' => new DateInterval('PT1H')]), $context),
        );
        static::assertFalse(
            ref('v')->lessThan(lit(new DateInterval('PT1H')))->eval(row(['v' => new DateInterval('PT5H')]), $context),
        );
        static::assertFalse(
            ref('v')->lessThan(lit(new DateInterval('PT1H')))->eval(row(['v' => new DateInterval('PT1H')]), $context),
        );
    }
}

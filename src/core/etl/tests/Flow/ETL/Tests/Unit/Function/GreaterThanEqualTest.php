<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateInterval;
use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException as ETLInvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;
use Flow\Types\Exception\InvalidArgumentException;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\list_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\ETL\DSL\time_entry;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;

final class GreaterThanEqualTest extends FlowTestCase
{
    public function test_greater_than_equal_arrays(): void
    {
        $context = flow_context();
        $type = type_list(type_integer());

        static::assertTrue(
            ref('v')->greaterThanEqual(lit([1, 0]))->eval(row(list_entry('v', [1, 9], $type)), $context),
        );
        static::assertTrue(
            ref('v')->greaterThanEqual(lit([1, 0]))->eval(row(list_entry('v', [1, 0], $type)), $context),
        );
        static::assertFalse(
            ref('v')->greaterThanEqual(lit([1, 9]))->eval(row(list_entry('v', [1, 0], $type)), $context),
        );
    }

    public function test_greater_than_equal_datetimes(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(datetime_entry('v', new DateTimeImmutable('2024-06-01'))), $context),
        );
        static::assertTrue(
            ref('v')
                ->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01')))
                ->eval(row(datetime_entry('v', new DateTimeImmutable('2024-01-01'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThanEqual(lit(new DateTimeImmutable('2024-06-01')))
                ->eval(row(datetime_entry('v', new DateTimeImmutable('2024-01-01'))), $context),
        );
    }

    public function test_greater_than_equal_floats(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThanEqual(lit(1.5))->eval(row(float_entry('v', 2.5)), $context));
        static::assertTrue(ref('v')->greaterThanEqual(lit(1.5))->eval(row(float_entry('v', 1.5)), $context));
        static::assertFalse(ref('v')->greaterThanEqual(lit(2.5))->eval(row(float_entry('v', 1.5)), $context));
    }

    public function test_greater_than_equal_integers(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThanEqual(lit(10))->eval(row(int_entry('v', 20)), $context));
        static::assertTrue(ref('v')->greaterThanEqual(lit(10))->eval(row(int_entry('v', 10)), $context));
        static::assertFalse(ref('v')->greaterThanEqual(lit(20))->eval(row(int_entry('v', 10)), $context));
    }

    public function test_greater_than_equal_returns_null_for_null_array(): void
    {
        $this->expectException(ETLInvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        ref('v')
            ->greaterThanEqual(lit([1, 0]))
            ->eval(row(list_entry('v', null, type_list(type_integer()))), flow_context());
    }

    public function test_greater_than_equal_returns_null_for_null_datetime(): void
    {
        $this->expectException(ETLInvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        ref('v')
            ->greaterThanEqual(lit(new DateTimeImmutable('2024-01-01')))
            ->eval(row(datetime_entry('v', null)), flow_context());
    }

    public function test_greater_than_equal_returns_null_for_null_left(): void
    {
        $this->expectException(ETLInvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        ref('v')->greaterThanEqual(lit(10))->eval(row(int_entry('v', null)), flow_context());
    }

    public function test_greater_than_equal_returns_null_for_null_right(): void
    {
        $this->expectException(ETLInvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        ref('v')
            ->greaterThanEqual(ref('other'))
            ->eval(row(int_entry('v', 10), int_entry('other', null)), flow_context());
    }

    public function test_greater_than_equal_returns_null_for_null_string(): void
    {
        $this->expectException(ETLInvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        ref('v')->greaterThanEqual(lit('a'))->eval(row(str_entry('v', null)), flow_context());
    }

    public function test_greater_than_equal_returns_null_for_null_time_interval(): void
    {
        $this->expectException(ETLInvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        ref('v')->greaterThanEqual(lit(new DateInterval('PT1H')))->eval(row(time_entry('v', null)), flow_context());
    }

    public function test_greater_than_equal_strings(): void
    {
        $context = flow_context();

        static::assertTrue(ref('v')->greaterThanEqual(lit('apple'))->eval(row(str_entry('v', 'banana')), $context));
        static::assertTrue(ref('v')->greaterThanEqual(lit('apple'))->eval(row(str_entry('v', 'apple')), $context));
        static::assertFalse(ref('v')->greaterThanEqual(lit('banana'))->eval(row(str_entry('v', 'apple')), $context));
    }

    public function test_greater_than_equal_throws_on_incompatible_types(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('v')->greaterThanEqual(lit('apple'))->eval(row(bool_entry('v', true)), flow_context());
    }

    public function test_greater_than_equal_time_intervals(): void
    {
        $context = flow_context();

        static::assertTrue(
            ref('v')
                ->greaterThanEqual(lit(new DateInterval('PT1H')))
                ->eval(row(time_entry('v', new DateInterval('PT5H'))), $context),
        );
        static::assertTrue(
            ref('v')
                ->greaterThanEqual(lit(new DateInterval('PT1H')))
                ->eval(row(time_entry('v', new DateInterval('PT1H'))), $context),
        );
        static::assertFalse(
            ref('v')
                ->greaterThanEqual(lit(new DateInterval('PT5H')))
                ->eval(row(time_entry('v', new DateInterval('PT1H'))), $context),
        );
    }
}

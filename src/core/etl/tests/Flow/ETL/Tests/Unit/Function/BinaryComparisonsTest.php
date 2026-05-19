<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use DateTimeImmutable;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\Contains;
use Flow\ETL\Function\EndsWith;
use Flow\ETL\Function\Equals;
use Flow\ETL\Function\ExecutionMode;
use Flow\ETL\Function\GreaterThan;
use Flow\ETL\Function\GreaterThanEqual;
use Flow\ETL\Function\IsIn;
use Flow\ETL\Function\IsNotNull;
use Flow\ETL\Function\IsNotNumeric;
use Flow\ETL\Function\IsNull;
use Flow\ETL\Function\IsNumeric;
use Flow\ETL\Function\IsType;
use Flow\ETL\Function\LessThan;
use Flow\ETL\Function\LessThanEqual;
use Flow\ETL\Function\NotEquals;
use Flow\ETL\Function\NotSame;
use Flow\ETL\Function\Same;
use Flow\ETL\Function\StartsWith;
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;
use function Flow\Types\DSL\type_string;

final class BinaryComparisonsTest extends FlowTestCase
{
    public function test_equals(): void
    {
        $row = row(
            int_entry('a', 100),
            int_entry('b', 100),
            int_entry('c', 10),
            datetime_entry('d', '2023-01-01 00:00:00 UTC'),
            datetime_entry('e', '2023-01-01 00:00:00 UTC'),
        );

        static::assertTrue((new Equals(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertTrue((new Equals(ref('d'), ref('e')))->eval($row, flow_context()));
        static::assertFalse((new Equals(ref('a'), ref('c')))->eval($row, flow_context()));
    }

    public function test_greater_than(): void
    {
        $row = row(
            int_entry('a', 100),
            int_entry('b', 100),
            int_entry('c', 10),
            datetime_entry('d', '2023-01-01 00:00:00 UTC'),
            datetime_entry('e', '2023-01-02 00:00:00 UTC'),
            int_entry('f', null),
        );

        static::assertTrue((new GreaterThan(ref('a'), ref('c')))->eval($row, flow_context()));
        static::assertNull((new GreaterThan(ref('a'), ref('f')))->eval($row, flow_context()));
        static::assertNull((new GreaterThan(ref('f'), ref('c')))->eval($row, flow_context()));
        static::assertNull((new GreaterThan(ref('f'), ref('f')))->eval($row, flow_context()));
        static::assertFalse((new GreaterThan(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertTrue((new GreaterThanEqual(ref('a'), ref('c')))->eval($row, flow_context()));
        static::assertTrue((new GreaterThanEqual(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertTrue((new GreaterThanEqual(ref('e'), ref('d')))->eval($row, flow_context()));
        static::assertTrue((new GreaterThanEqual(
            ref('e'),
            lit(new DateTimeImmutable('2022-01-01 00:00:00 UTC')),
        ))->eval($row, flow_context()));
        static::assertFalse((new GreaterThanEqual(
            ref('e'),
            lit(new DateTimeImmutable('2024-01-01 00:00:00 UTC')),
        ))->eval($row, flow_context()));
        static::assertNull((new GreaterThanEqual(ref('a'), ref('f')))->eval($row, flow_context()));
        static::assertNull((new GreaterThanEqual(ref('f'), ref('c')))->eval($row, flow_context()));
        static::assertNull((new GreaterThanEqual(ref('f'), ref('f')))->eval($row, flow_context()));
    }

    public function test_greater_than_equal_with_null_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThanEqual function requires non-null values');

        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $row = row(int_entry('a', 100), int_entry('f', null));
        (new GreaterThanEqual(ref('a'), ref('f')))->eval($row, $context);
    }

    public function test_greater_than_with_null_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('GreaterThan function requires non-null values');

        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $row = row(int_entry('a', 100), int_entry('f', null));
        (new GreaterThan(ref('a'), ref('f')))->eval($row, $context);
    }

    public function test_is_in(): void
    {
        $row = Row::with(
            json_entry('a', [1, 2, 3, 4, 5]),
            json_entry('b', ['a', 'b', 'c']),
            str_entry('c', 'another'),
            int_entry('d', 4),
            str_entry('e', 'b'),
        );

        static::assertTrue((new IsIn(ref('a'), lit(1)))->eval($row, flow_context()));
        static::assertFalse((new IsIn(ref('a'), lit(10)))->eval($row, flow_context()));
        static::assertTrue((new IsIn(ref('a'), ref('d')))->eval($row, flow_context()));
        static::assertTrue((new IsIn(ref('b'), ref('e')))->eval($row, flow_context()));
    }

    public function test_is_in_with_null_array_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('IsIn function requires non-null array');

        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $row = row(int_entry('a', null), int_entry('d', 1));
        (new IsIn(ref('a'), ref('d')))->eval($row, $context);
    }

    public function test_is_numeric(): void
    {
        $row = row(int_entry('a', 100), int_entry('b', null));
        static::assertTrue((new IsNumeric(ref('a')))->eval($row, flow_context()));
        static::assertFalse((new IsNumeric(ref('b')))->eval($row, flow_context()));
        static::assertFalse((new IsNotNumeric(ref('a')))->eval($row, flow_context()));
        static::assertTrue((new IsNotNumeric(ref('b')))->eval($row, flow_context()));
        static::assertTrue((new IsNotNumeric(lit(null)))->eval($row, flow_context()));
        static::assertTrue((new IsNumeric(lit(1000)))->eval($row, flow_context()));
    }

    public function test_is_type(): void
    {
        $row = row(int_entry('a', 100), int_entry('b', null));

        static::assertTrue((new IsType(ref('a'), 'integer', 'string'))->eval($row, flow_context()));
        static::assertFalse((new IsType(ref('a'), type_string()))->eval($row, flow_context()));
    }

    public function test_is_type_with_non_existing_type_class(): void
    {
        $this->expectExceptionMessage('Unknown type \'aaa\'');

        $row = row(int_entry('a', 100), int_entry('b', null));

        static::assertFalse((new IsType(ref('a'), 'aaa'))->eval($row, flow_context()));
    }

    public function test_less_than(): void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10), int_entry('d', null));

        static::assertFalse((new LessThan(ref('a'), ref('c')))->eval($row, flow_context()));
        static::assertNull((new LessThan(ref('a'), ref('d')))->eval($row, flow_context()));
        static::assertNull((new LessThan(ref('d'), ref('d')))->eval($row, flow_context()));
        static::assertNull((new LessThan(ref('d'), ref('c')))->eval($row, flow_context()));
        static::assertFalse((new LessThan(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertTrue((new LessThanEqual(ref('c'), ref('a')))->eval($row, flow_context()));
        static::assertTrue((new LessThanEqual(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertNull((new LessThanEqual(ref('a'), ref('d')))->eval($row, flow_context()));
        static::assertNull((new LessThanEqual(ref('d'), ref('c')))->eval($row, flow_context()));
        static::assertNull((new LessThanEqual(ref('d'), ref('d')))->eval($row, flow_context()));
    }

    public function test_less_than_equal_with_null_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('LessThanEqual function requires non-null values');

        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $row = row(int_entry('a', 100), int_entry('d', null));
        (new LessThanEqual(ref('a'), ref('d')))->eval($row, $context);
    }

    public function test_less_than_with_null_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('LessThan function requires non-null values');

        $context = flow_context();
        $context->functions()->setMode(ExecutionMode::STRICT);

        $row = row(int_entry('a', 100), int_entry('d', null));
        (new LessThan(ref('a'), ref('d')))->eval($row, $context);
    }

    public function test_not_equals(): void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10));

        static::assertFalse((new NotEquals(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertTrue((new NotEquals(ref('a'), ref('c')))->eval($row, flow_context()));
    }

    public function test_not_same(): void
    {
        $row = row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10));

        static::assertTrue((new NotSame(ref('a'), ref('c')))->eval($row, flow_context()));
        static::assertFalse((new NotSame(ref('a'), ref('b')))->eval($row, flow_context()));
    }

    public function test_null(): void
    {
        $row = row(int_entry('a', 100), int_entry('b', null));

        static::assertFalse((new IsNull(ref('a')))->eval($row, flow_context()));
        static::assertTrue((new IsNull(ref('b')))->eval($row, flow_context()));
        static::assertTrue((new IsNotNull(ref('a')))->eval($row, flow_context()));
        static::assertFalse((new IsNotNull(ref('b')))->eval($row, flow_context()));
        static::assertTrue((new IsNull(lit(null)))->eval($row, flow_context()));
        static::assertTrue((new IsNotNull(lit(1000)))->eval($row, flow_context()));
    }

    public function test_same(): void
    {
        $row = row(
            int_entry('a', 100),
            int_entry('b', 100),
            int_entry('c', 10),
            datetime_entry('d', '2023-01-01 00:00:00 UTC'),
            datetime_entry('e', '2023-01-01 00:00:00 UTC'),
        );

        static::assertTrue((new Same(ref('a'), ref('b')))->eval($row, flow_context()));
        static::assertFalse((new Same(ref('d'), ref('e')))->eval($row, flow_context()));
        static::assertFalse((new Same(ref('a'), ref('c')))->eval($row, flow_context()));
    }

    public function test_starts_ends_with(): void
    {
        $row = Row::with(
            str_entry('a', 'some not too long string'),
            str_entry('b', 'another not too long text'),
            str_entry('c', 'another'),
            str_entry('d', 'text'),
        );

        static::assertTrue((new StartsWith(ref('a'), lit('some not')))->eval($row, flow_context()));
        static::assertTrue((new EndsWith(ref('a'), lit('long string')))->eval($row, flow_context()));
        static::assertTrue((new StartsWith(ref('b'), ref('c')))->eval($row, flow_context()));
        static::assertTrue((new EndsWith(ref('b'), ref('d')))->eval($row, flow_context()));
        static::assertTrue((new Contains(ref('a'), lit('too long')))->eval($row, flow_context()));
        static::assertFalse((new Contains(ref('a'), lit('blablabla')))->eval($row, flow_context()));
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{datetime_entry, int_entry, json_entry, lit, ref, str_entry, type_string};
use Flow\ETL\Function\{Contains, EndsWith, Equals, GreaterThan, GreaterThanEqual, IsIn, IsNotNull, IsNotNumeric, IsNull, IsNumeric, IsType, LessThan, LessThanEqual, NotEquals, NotSame, Same, StartsWith};
use Flow\ETL\Row;
use Flow\ETL\Tests\FlowTestCase;

final class BinaryComparisonsTest extends FlowTestCase
{
    public function test_equals() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10), datetime_entry('d', '2023-01-01 00:00:00 UTC'), datetime_entry('e', '2023-01-01 00:00:00 UTC'));

        self::assertTrue(
            (new Equals(ref('a'), ref('b')))->eval($row)
        );
        self::assertTrue(
            (new Equals(ref('d'), ref('e')))->eval($row)
        );
        self::assertFalse(
            (new Equals(ref('a'), ref('c')))->eval($row)
        );
    }

    public function test_greater_than() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10), datetime_entry('d', '2023-01-01 00:00:00 UTC'), datetime_entry('e', '2023-01-02 00:00:00 UTC'), int_entry('f', null));

        self::assertTrue((new GreaterThan(ref('a'), ref('c')))->eval($row));
        self::assertFalse((new GreaterThan(ref('a'), ref('f')))->eval($row));
        self::assertFalse((new GreaterThan(ref('f'), ref('c')))->eval($row));
        self::assertFalse((new GreaterThan(ref('f'), ref('f')))->eval($row));
        self::assertFalse((new GreaterThan(ref('a'), ref('b')))->eval($row));
        self::assertTrue((new GreaterThanEqual(ref('a'), ref('c')))->eval($row));
        self::assertTrue((new GreaterThanEqual(ref('a'), ref('b')))->eval($row));
        self::assertTrue((new GreaterThanEqual(ref('e'), ref('d')))->eval($row));
        self::assertTrue((new GreaterThanEqual(ref('e'), lit(new \DateTimeImmutable('2022-01-01 00:00:00 UTC'))))->eval($row));
        self::assertFalse((new GreaterThanEqual(ref('e'), lit(new \DateTimeImmutable('2024-01-01 00:00:00 UTC'))))->eval($row));
        self::assertFalse((new GreaterThanEqual(ref('a'), ref('f')))->eval($row));
        self::assertFalse((new GreaterThanEqual(ref('f'), ref('c')))->eval($row));
        self::assertFalse((new GreaterThanEqual(ref('f'), ref('f')))->eval($row));

    }

    public function test_is_in() : void
    {
        $row = Row::with(
            json_entry('a', [1, 2, 3, 4, 5]),
            json_entry('b', ['a', 'b', 'c']),
            str_entry('c', 'another'),
            int_entry('d', 4),
            str_entry('e', 'b'),
        );

        self::assertTrue((new IsIn(ref('a'), lit(1)))->eval($row));
        self::assertFalse((new IsIn(ref('a'), lit(10)))->eval($row));
        self::assertTrue((new IsIn(ref('a'), ref('d')))->eval($row));
        self::assertTrue((new IsIn(ref('b'), ref('e')))->eval($row));
    }

    public function test_is_numeric() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', null));
        self::assertTrue((new IsNumeric(ref('a')))->eval($row));
        self::assertFalse((new IsNumeric(ref('b')))->eval($row));
        self::assertFalse((new IsNotNumeric(ref('a')))->eval($row));
        self::assertTrue((new IsNotNumeric(ref('b')))->eval($row));
        self::assertTrue((new IsNotNumeric(lit(null)))->eval($row));
        self::assertTrue((new IsNumeric(lit(1000)))->eval($row));
    }

    public function test_is_type() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', null));

        self::assertTrue((new IsType(ref('a'), 'integer', 'string'))->eval($row));
        self::assertFalse((new IsType(ref('a'), type_string()))->eval($row));
    }

    public function test_is_type_with_non_existing_type_class() : void
    {
        $this->expectExceptionMessage('Unknown type \'aaa\'');

        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', null));

        self::assertFalse((new IsType(ref('a'), 'aaa'))->eval($row));
    }

    public function test_less_than() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10), int_entry('d', null));

        self::assertFalse((new LessThan(ref('a'), ref('c')))->eval($row));
        self::assertFalse((new LessThan(ref('a'), ref('d')))->eval($row));
        self::assertFalse((new LessThan(ref('d'), ref('d')))->eval($row));
        self::assertFalse((new LessThan(ref('d'), ref('c')))->eval($row));
        self::assertFalse((new LessThan(ref('a'), ref('b')))->eval($row));
        self::assertTrue((new LessThanEqual(ref('c'), ref('a')))->eval($row));
        self::assertTrue((new LessThanEqual(ref('a'), ref('b')))->eval($row));
        self::assertFalse((new LessThanEqual(ref('a'), ref('d')))->eval($row));
        self::assertFalse((new LessThanEqual(ref('d'), ref('c')))->eval($row));
        self::assertFalse((new LessThanEqual(ref('d'), ref('d')))->eval($row));
    }

    public function test_not_equals() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10));

        self::assertFalse(
            (new NotEquals(ref('a'), ref('b')))->eval($row)
        );
        self::assertTrue(
            (new NotEquals(ref('a'), ref('c')))->eval($row)
        );
    }

    public function test_not_same() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10));

        self::assertTrue(
            (new NotSame(ref('a'), ref('c')))->eval($row)
        );
        self::assertFalse(
            (new NotSame(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_null() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', null));

        self::assertFalse((new IsNull(ref('a')))->eval($row));
        self::assertTrue((new IsNull(ref('b')))->eval($row));
        self::assertTrue((new IsNotNull(ref('a')))->eval($row));
        self::assertFalse((new IsNotNull(ref('b')))->eval($row));
        self::assertTrue((new IsNull(lit(null)))->eval($row));
        self::assertTrue((new IsNotNull(lit(1000)))->eval($row));
    }

    public function test_same() : void
    {
        $row = \Flow\ETL\DSL\row(int_entry('a', 100), int_entry('b', 100), int_entry('c', 10), datetime_entry('d', '2023-01-01 00:00:00 UTC'), datetime_entry('e', '2023-01-01 00:00:00 UTC'));

        self::assertTrue(
            (new Same(ref('a'), ref('b')))->eval($row)
        );
        self::assertFalse(
            (new Same(ref('d'), ref('e')))->eval($row)
        );
        self::assertFalse(
            (new Same(ref('a'), ref('c')))->eval($row)
        );
    }

    public function test_starts_ends_with() : void
    {
        $row = Row::with(
            str_entry('a', 'some not too long string'),
            str_entry('b', 'another not too long text'),
            str_entry('c', 'another'),
            str_entry('d', 'text')
        );

        self::assertTrue((new StartsWith(ref('a'), lit('some not')))->eval($row));
        self::assertTrue((new EndsWith(ref('a'), lit('long string')))->eval($row));
        self::assertTrue((new StartsWith(ref('b'), ref('c')))->eval($row));
        self::assertTrue((new EndsWith(ref('b'), ref('d')))->eval($row));
        self::assertTrue((new Contains(ref('a'), lit('too long')))->eval($row));
        self::assertFalse((new Contains(ref('a'), lit('blablabla')))->eval($row));
    }
}

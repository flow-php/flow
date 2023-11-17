<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Function\Contains;
use Flow\ETL\Function\EndsWith;
use Flow\ETL\Function\Equals;
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
use Flow\ETL\Row\Entry\IntegerEntry;
use Flow\ETL\Row\Entry\StringEntry;
use PHPUnit\Framework\TestCase;

final class BinaryComparisonsTest extends TestCase
{
    public function test_equals() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100), Entry::integer('c', 10), Entry::datetime('d', '2023-01-01 00:00:00 UTC'), Entry::datetime('e', '2023-01-01 00:00:00 UTC'));

        $this->assertTrue(
            (new Equals(ref('a'), ref('b')))->eval($row)
        );
        $this->assertTrue(
            (new Equals(ref('d'), ref('e')))->eval($row)
        );
        $this->assertFalse(
            (new Equals(ref('a'), ref('c')))->eval($row)
        );
    }

    public function test_greater_than() : void
    {
        $row = Row::create(
            Entry::integer('a', 100),
            Entry::integer('b', 100),
            Entry::integer('c', 10),
            Entry::datetime('d', '2023-01-01 00:00:00 UTC'),
            Entry::datetime('e', '2023-01-02 00:00:00 UTC'),
        );

        $this->assertTrue((new GreaterThan(ref('a'), ref('c')))->eval($row));
        $this->assertFalse((new GreaterThan(ref('a'), ref('b')))->eval($row));
        $this->assertTrue((new GreaterThanEqual(ref('a'), ref('c')))->eval($row));
        $this->assertTrue((new GreaterThanEqual(ref('a'), ref('b')))->eval($row));
        $this->assertTrue((new GreaterThanEqual(ref('e'), ref('d')))->eval($row));
        $this->assertTrue((new GreaterThanEqual(ref('e'), lit(new \DateTimeImmutable('2022-01-01 00:00:00 UTC'))))->eval($row));
        $this->assertFalse((new GreaterThanEqual(ref('e'), lit(new \DateTimeImmutable('2024-01-01 00:00:00 UTC'))))->eval($row));
    }

    public function test_is_in() : void
    {
        $row = Row::with(
            Entry::array('a', [1, 2, 3, 4, 5]),
            Entry::array('b', ['a', 'b', 'c']),
            Entry::str('c', 'another'),
            Entry::int('d', 4),
            Entry::str('e', 'b'),
        );

        $this->assertTrue((new IsIn(ref('a'), lit(1)))->eval($row));
        $this->assertFalse((new IsIn(ref('a'), lit(10)))->eval($row));
        $this->assertTrue((new IsIn(ref('a'), ref('d')))->eval($row));
        $this->assertTrue((new IsIn(ref('b'), ref('e')))->eval($row));
    }

    public function test_is_numeric() : void
    {
        $row = Row::create(
            Entry::integer('a', 100),
            Entry::null('b'),
        );
        $this->assertTrue((new IsNumeric(ref('a')))->eval($row));
        $this->assertFalse((new IsNumeric(ref('b')))->eval($row));
        $this->assertFalse((new IsNotNumeric(ref('a')))->eval($row));
        $this->assertTrue((new IsNotNumeric(ref('b')))->eval($row));
        $this->assertTrue((new IsNotNumeric(lit(null)))->eval($row));
        $this->assertTrue((new IsNumeric(lit(1000)))->eval($row));
    }

    public function test_is_type() : void
    {
        $row = Row::create(
            Entry::integer('a', 100),
            Entry::null('b'),
        );

        $this->assertTrue((new IsType(ref('a'), IntegerEntry::class, StringEntry::class))->eval($row));
        $this->assertFalse((new IsType(ref('a'), StringEntry::class))->eval($row));
    }

    public function test_is_type_with_non_existing_type_class() : void
    {
        $this->expectExceptionMessage('"aaa" is not valid Entry Type class');

        $row = Row::create(
            Entry::integer('a', 100),
            Entry::null('b'),
        );

        $this->assertFalse((new IsType(ref('a'), 'aaa'))->eval($row));
    }

    public function test_less_than() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100), Entry::integer('c', 10));

        $this->assertFalse(
            (new LessThan(ref('a'), ref('c')))->eval($row)
        );
        $this->assertFalse(
            (new LessThan(ref('a'), ref('b')))->eval($row)
        );
        $this->assertTrue(
            (new LessThanEqual(ref('c'), ref('a')))->eval($row)
        );
        $this->assertTrue(
            (new LessThanEqual(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_not_equals() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100), Entry::integer('c', 10));

        $this->assertFalse(
            (new NotEquals(ref('a'), ref('b')))->eval($row)
        );
        $this->assertTrue(
            (new NotEquals(ref('a'), ref('c')))->eval($row)
        );
    }

    public function test_not_same() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100), Entry::integer('c', 10));

        $this->assertTrue(
            (new NotSame(ref('a'), ref('c')))->eval($row)
        );
        $this->assertFalse(
            (new NotSame(ref('a'), ref('b')))->eval($row)
        );
    }

    public function test_null() : void
    {
        $row = Row::create(
            Entry::integer('a', 100),
            Entry::null('b'),
        );

        $this->assertFalse((new IsNull(ref('a')))->eval($row));
        $this->assertTrue((new IsNull(ref('b')))->eval($row));
        $this->assertTrue((new IsNotNull(ref('a')))->eval($row));
        $this->assertFalse((new IsNotNull(ref('b')))->eval($row));
        $this->assertTrue((new IsNull(lit(null)))->eval($row));
        $this->assertTrue((new IsNotNull(lit(1000)))->eval($row));
    }

    public function test_same() : void
    {
        $row = Row::create(Entry::integer('a', 100), Entry::integer('b', 100), Entry::integer('c', 10), Entry::datetime('d', '2023-01-01 00:00:00 UTC'), Entry::datetime('e', '2023-01-01 00:00:00 UTC'));

        $this->assertTrue(
            (new Same(ref('a'), ref('b')))->eval($row)
        );
        $this->assertFalse(
            (new Same(ref('d'), ref('e')))->eval($row)
        );
        $this->assertFalse(
            (new Same(ref('a'), ref('c')))->eval($row)
        );
    }

    public function test_starts_ends_with() : void
    {
        $row = Row::with(
            Entry::str('a', 'some not too long string'),
            Entry::str('b', 'another not too long text'),
            Entry::str('c', 'another'),
            Entry::str('d', 'text')
        );

        $this->assertTrue((new StartsWith(ref('a'), lit('some not')))->eval($row));
        $this->assertTrue((new EndsWith(ref('a'), lit('long string')))->eval($row));
        $this->assertTrue((new StartsWith(ref('b'), ref('c')))->eval($row));
        $this->assertTrue((new EndsWith(ref('b'), ref('d')))->eval($row));
        $this->assertTrue((new Contains(ref('a'), lit('too long')))->eval($row));
        $this->assertFalse((new Contains(ref('a'), lit('blablabla')))->eval($row));
    }
}

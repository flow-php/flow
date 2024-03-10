<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{between, int_entry, lit, ref, row};
use Flow\ETL\Function\Between\Boundary;
use PHPUnit\Framework\TestCase;

final class BetweenTest extends TestCase
{
    public function test_between_exclusive() : void
    {
        self::assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 11)))
        );
        self::assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 49)))
        );
        self::assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 10)))
        );
        self::assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 50)))
        );
    }

    public function test_between_inclusive() : void
    {
        self::assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 10)))
        );
        self::assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 50)))
        );
        self::assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 9)))
        );
        self::assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 51)))
        );
    }

    public function test_between_left_inclusive() : void
    {
        self::assertTrue(
            between(ref('value'), lit(10), lit(50))->eval(row(int_entry('value', 10)))
        );
        self::assertFalse(
            between(ref('value'), lit(10), lit(50))->eval(row(int_entry('value', 9)))
        );
    }

    public function test_between_right_inclusive() : void
    {
        self::assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(row(int_entry('value', 50)))
        );
        self::assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(row(int_entry('value', 51)))
        );
    }
}

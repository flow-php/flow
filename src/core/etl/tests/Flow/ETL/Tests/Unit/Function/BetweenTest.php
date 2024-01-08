<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\between;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use Flow\ETL\Function\Between\Boundary;
use PHPUnit\Framework\TestCase;

final class BetweenTest extends TestCase
{
    public function test_between_exclusive() : void
    {
        $this->assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 11)))
        );
        $this->assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 49)))
        );
        $this->assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 10)))
        );
        $this->assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::EXCLUSIVE)->eval(row(int_entry('value', 50)))
        );
    }

    public function test_between_inclusive() : void
    {
        $this->assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 10)))
        );
        $this->assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 50)))
        );
        $this->assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 9)))
        );
        $this->assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::INCLUSIVE)->eval(row(int_entry('value', 51)))
        );
    }

    public function test_between_left_inclusive() : void
    {
        $this->assertTrue(
            between(ref('value'), lit(10), lit(50))->eval(row(int_entry('value', 10)))
        );
        $this->assertFalse(
            between(ref('value'), lit(10), lit(50))->eval(row(int_entry('value', 9)))
        );
    }

    public function test_between_right_inclusive() : void
    {
        $this->assertTrue(
            between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(row(int_entry('value', 50)))
        );
        $this->assertFalse(
            between(ref('value'), lit(10), lit(50), Boundary::RIGHT_INCLUSIVE)->eval(row(int_entry('value', 51)))
        );
    }
}

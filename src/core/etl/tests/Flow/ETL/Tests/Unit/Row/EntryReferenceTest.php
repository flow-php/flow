<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class EntryReferenceTest extends TestCase
{
    public function test_executing_equals_expression() : void
    {
        $ref = ref('a')->equals(ref('b'));

        $this->assertTrue(
            $ref->eval(Row::create(int_entry('a', 1), int_entry('b', 1)))
        );
    }

    public function test_executing_expression() : void
    {
        $ref = ref('b')->literal(100);

        $this->assertSame(
            100,
            $ref->eval(Row::create(int_entry('a', 1)))
        );
    }

    public function test_is_even() : void
    {
        $ref = ref('a')->isEven();

        $this->assertFalse(
            $ref->eval(Row::create(int_entry('a', 1)))
        );

        $this->assertTrue(
            $ref->eval(Row::create(int_entry('a', 2)))
        );
    }

    public function test_is_odd() : void
    {
        $ref = ref('a')->isOdd();

        $this->assertTrue(
            $ref->eval(Row::create(int_entry('a', 1)))
        );

        $this->assertFalse(
            $ref->eval(Row::create(int_entry('a', 2)))
        );
    }
}

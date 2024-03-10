<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_entry, array_expand, int_entry, ref};
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayExpandTest extends TestCase
{
    public function test_expand_both() : void
    {
        $row = Row::create(
            array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]),
        );

        self::assertSame(
            [
                ['a' => 1],
                ['b' => 2],
                ['c' => 3],
            ],
            array_expand(ref('array'), ArrayExpand::BOTH)->eval($row)
        );
    }

    public function test_expand_keys() : void
    {
        $row = Row::create(
            array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]),
        );

        self::assertSame(
            ['a', 'b', 'c'],
            array_expand(ref('array'), ArrayExpand::KEYS)->eval($row)
        );
    }

    public function test_expand_values() : void
    {
        $row = Row::create(
            array_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]),
        );

        self::assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            array_expand(ref('array'))->eval($row)
        );
    }

    public function test_for_not_array_entry() : void
    {
        self::assertNull(
            array_expand(ref('integer_entry'))->eval(Row::create(int_entry('integer_entry', 1)))
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{array_expand, int_entry, json_entry, ref};
use Flow\ETL\Function\ArrayExpand\ArrayExpand;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayExpandTest extends FlowTestCase
{
    public function test_expand_both() : void
    {
        $row = row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]));

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
        $row = row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]));

        self::assertSame(
            ['a', 'b', 'c'],
            array_expand(ref('array'), ArrayExpand::KEYS)->eval($row)
        );
    }

    public function test_expand_values() : void
    {
        $row = row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3]));

        self::assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            array_expand(ref('array'))->eval($row)
        );
    }

    public function test_for_not_array_entry() : void
    {
        self::assertNull(
            array_expand(ref('integer_entry'))->eval(row(int_entry('integer_entry', 1)))
        );
    }
}

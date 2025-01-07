<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{int_entry, json_entry, lit, ref};
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Tests\FlowTestCase;

final class ArrayMergeTest extends FlowTestCase
{
    public function test_array_merge_two_array_row_entries() : void
    {
        self::assertSame(
            ['a' => 1, 'b' => 2],
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    row(json_entry('a', ['a' => 1]), json_entry('b', ['b' => 2])),
                )
        );
    }

    public function test_array_merge_two_lit_functions() : void
    {
        $function = new ArrayMerge(
            lit(['a' => 1]),
            lit(['b' => 2])
        );

        self::assertSame(['a' => 1, 'b' => 2], $function->eval(row()));
    }

    public function test_array_merge_when_left_side_is_not_an_array() : void
    {
        self::assertNull(
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    row(int_entry('a', 1), json_entry('b', ['b' => 2])),
                )
        );
    }

    public function test_array_merge_when_right_side_is_not_an_array() : void
    {
        self::assertNull(
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    row(json_entry('a', ['a' => 1]), int_entry('b', 2)),
                )
        );
    }
}

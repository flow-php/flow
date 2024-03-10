<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_entry, int_entry, lit, ref};
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayMergeTest extends TestCase
{
    public function test_array_merge_two_array_row_entries() : void
    {
        self::assertSame(
            ['a' => 1, 'b' => 2],
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    Row::create(
                        array_entry('a', ['a' => 1]),
                        array_entry('b', ['b' => 2]),
                    ),
                )
        );
    }

    public function test_array_merge_two_lit_functions() : void
    {
        $function = new ArrayMerge(
            lit(['a' => 1]),
            lit(['b' => 2])
        );

        self::assertSame(['a' => 1, 'b' => 2], $function->eval(Row::create()));
    }

    public function test_array_merge_when_left_side_is_not_an_array() : void
    {
        self::assertNull(
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    Row::create(
                        int_entry('a', 1),
                        array_entry('b', ['b' => 2]),
                    ),
                )
        );
    }

    public function test_array_merge_when_right_side_is_not_an_array() : void
    {
        self::assertNull(
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    Row::create(
                        array_entry('a', ['a' => 1]),
                        int_entry('b', 2),
                    ),
                )
        );
    }
}

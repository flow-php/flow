<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class ArrayMergeTest extends TestCase
{
    public function test_array_merge_two_array_row_entries() : void
    {
        $this->assertSame(
            ['a' => 1, 'b' => 2],
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    Row::create(
                        Entry::array('a', ['a' => 1]),
                        Entry::array('b', ['b' => 2]),
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

        $this->assertSame(['a' => 1, 'b' => 2], $function->eval(Row::create()));
    }

    public function test_array_merge_when_left_side_is_not_an_array() : void
    {
        $this->assertNull(
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    Row::create(
                        Entry::int('a', 1),
                        Entry::array('b', ['b' => 2]),
                    ),
                )
        );
    }

    public function test_array_merge_when_right_side_is_not_an_array() : void
    {
        $this->assertNull(
            ref('a')->arrayMerge(ref('b'))
                ->eval(
                    Row::create(
                        Entry::array('a', ['a' => 1]),
                        Entry::int('b', 2),
                    ),
                )
        );
    }
}

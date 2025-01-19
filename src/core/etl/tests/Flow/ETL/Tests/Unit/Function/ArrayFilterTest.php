<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{int_entry, list_entry, lit, ref, string_entry, type_int, type_list};
use Flow\ETL\Tests\FlowTestCase;

final class ArrayFilterTest extends FlowTestCase
{
    public function test_array_filter() : void
    {
        self::assertSame(
            [1],
            ref('list')->arrayFilter(lit(2))
                ->eval(
                    row(list_entry('list', [1, 2], type_list(type_int()))),
                )
        );
    }

    public function test_array_filter_by_entry_reference() : void
    {
        self::assertSame(
            [1],
            ref('list')->arrayFilter(ref('int'))
                ->eval(
                    row(
                        list_entry('list', [1, 2], type_list(type_int())),
                        int_entry('int', 2)
                    ),
                )
        );
    }

    public function test_array_filter_not_existing_value() : void
    {
        self::assertSame(
            [1, 2],
            ref('list')->arrayFilter(lit(5))
                ->eval(
                    row(list_entry('list', [1, 2], type_list(type_int()))),
                )
        );
    }

    public function test_array_filter_on_non_array() : void
    {
        self::assertNull(
            ref('map')->arrayFilter(lit(1))
                ->eval(
                    row(string_entry('map', 'test')),
                )
        );
    }
}

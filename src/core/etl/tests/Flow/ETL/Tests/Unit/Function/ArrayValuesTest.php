<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{
    map_entry,
    ref,
    string_entry,
    type_int,
    type_map,
    type_string};
use Flow\ETL\Tests\FlowTestCase;

final class ArrayValuesTest extends FlowTestCase
{
    public function test_array_values() : void
    {
        self::assertSame(
            [1, 2],
            ref('map')->arrayValues()
                ->eval(
                    row(map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_int()))),
                )
        );
    }

    public function test_array_values_on_non_array() : void
    {
        self::assertNull(
            ref('map')->arrayValues()
                ->eval(
                    row(string_entry('map', 'test')),
                )
        );
    }
}

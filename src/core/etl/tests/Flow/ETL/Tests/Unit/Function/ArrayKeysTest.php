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

final class ArrayKeysTest extends FlowTestCase
{
    public function test_array_keys() : void
    {
        self::assertSame(
            ['a', 'b'],
            ref('map')->arrayKeys()
                ->eval(
                    row(map_entry('map', ['a' => 1, 'b' => 2], type_map(type_string(), type_int()))),
                )
        );
    }

    public function test_array_keys_on_non_array() : void
    {
        self::assertNull(
            ref('map')->arrayKeys()
                ->eval(
                    row(string_entry('map', 'test')),
                )
        );
    }
}

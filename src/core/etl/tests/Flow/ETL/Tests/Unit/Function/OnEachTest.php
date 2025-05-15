<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{json_entry, ref, row};
use function Flow\Types\DSL\type_string;
use Flow\ETL\Tests\FlowTestCase;

final class OnEachTest extends FlowTestCase
{
    public function test_executing_function_on_each_value_from_array() : void
    {
        self::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')->onEach(ref('element')->cast(type_string()))
                ->eval(
                    row(
                        json_entry(
                            'array',
                            [1, 2, 3, 4, 5]
                        )
                    )
                ),
        );
    }

    public function test_executing_function_on_each_value_from_empty_array() : void
    {
        self::assertSame(
            [],
            ref('array')->onEach(ref('element')->cast(type_string()))
                ->eval(
                    row(
                        json_entry(
                            'array',
                            []
                        )
                    )
                ),
        );
    }

    public function test_executing_function_on_each_value_with_preserving_keys() : void
    {
        self::assertSame(
            ['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5'],
            ref('array')->onEach(ref('element')->cast(type_string()), true)
                ->eval(
                    row(
                        json_entry(
                            'array',
                            ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]
                        )
                    )
                ),
        );
    }

    public function test_executing_function_on_each_value_without_preserving_keys() : void
    {
        self::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')->onEach(ref('element')->cast(type_string()), false)
                ->eval(
                    row(
                        json_entry(
                            'array',
                            ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]
                        )
                    )
                ),
        );
    }
}

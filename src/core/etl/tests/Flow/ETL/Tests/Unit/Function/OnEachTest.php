<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{array_entry, ref, row, type_string};
use Flow\ETL\Adapter\Elasticsearch\Tests\Integration\TestCase;

final class OnEachTest extends TestCase
{
    public function test_executing_function_on_each_value_from_array() : void
    {
        self::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')->onEach(ref('element')->cast(type_string()))
                ->eval(
                    row(
                        array_entry(
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
                        array_entry(
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
                        array_entry(
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
                        array_entry(
                            'array',
                            ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5]
                        )
                    )
                ),
        );
    }
}

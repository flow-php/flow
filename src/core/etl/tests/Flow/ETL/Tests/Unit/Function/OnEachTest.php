<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\Types\DSL\type_string;

final class OnEachTest extends FlowTestCase
{
    public function test_a_throwing_element_is_not_swallowed(): void
    {
        $this->expectException(InvalidArgumentException::class);

        ref('array')
            ->onEach(ref('element')->upper())
            ->eval(row(json_entry('array', ['a', 'b', ['nested' => 1], 'd'])), flow_context());
    }

    public function test_executing_function_on_each_value_from_array(): void
    {
        static::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')
                ->onEach(ref('element')->cast(type_string()))
                ->eval(row(json_entry('array', [1, 2, 3, 4, 5])), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_from_empty_array(): void
    {
        static::assertSame(
            [],
            ref('array')
                ->onEach(ref('element')->cast(type_string()))
                ->eval(row(json_entry('array', [])), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_with_preserving_keys(): void
    {
        static::assertSame(
            ['a' => '1', 'b' => '2', 'c' => '3', 'd' => '4', 'e' => '5'],
            ref('array')
                ->onEach(ref('element')->cast(type_string()), true)
                ->eval(row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5])), flow_context()),
        );
    }

    public function test_executing_function_on_each_value_without_preserving_keys(): void
    {
        static::assertSame(
            ['1', '2', '3', '4', '5'],
            ref('array')
                ->onEach(ref('element')->cast(type_string()), false)
                ->eval(row(json_entry('array', ['a' => 1, 'b' => 2, 'c' => 3, 'd' => 4, 'e' => 5])), flow_context()),
        );
    }
}

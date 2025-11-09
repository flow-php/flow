<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{combine, flow_context, lit};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CombineTest extends FlowTestCase
{
    public function test_array_combine() : void
    {
        self::assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c']), lit([1, 2, 3]))->eval(row(), flow_context()),
        );
    }

    public function test_array_combine_when_arrays_are_empty() : void
    {
        self::assertSame(
            [],
            combine(lit([]), lit([]))->eval(row(), flow_context()),
        );
    }

    public function test_array_combine_when_keys_are_not_array() : void
    {
        self::assertNull(
            combine(lit('a'), lit([1, 2, 3]))->eval(row(), flow_context()),
        );
    }

    public function test_array_combine_when_keys_are_not_unique() : void
    {
        self::assertSame(
            ['a' => 4, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c', 'a']), lit([1, 2, 3, 4]))->eval(row(), flow_context()),
        );
    }

    public function test_array_combine_when_one_of_arrays_is_empty() : void
    {
        self::assertNull(
            combine(lit(['a', 'b', 'c']), lit([]))->eval(row(), flow_context()),
        );
    }
}

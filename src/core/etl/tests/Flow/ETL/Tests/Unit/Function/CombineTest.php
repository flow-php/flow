<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{combine, lit};
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class CombineTest extends TestCase
{
    public function test_array_combine() : void
    {
        self::assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c']), lit([1, 2, 3]))->eval(Row::create()),
        );
    }

    public function test_array_combine_when_arrays_are_empty() : void
    {
        self::assertSame(
            [],
            combine(lit([]), lit([]))->eval(Row::create()),
        );
    }

    public function test_array_combine_when_keys_are_not_array() : void
    {
        self::assertNull(
            combine(lit('a'), lit([1, 2, 3]))->eval(Row::create()),
        );
    }

    public function test_array_combine_when_keys_are_not_unique() : void
    {
        self::assertSame(
            ['a' => 4, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c', 'a']), lit([1, 2, 3, 4]))->eval(Row::create()),
        );
    }

    public function test_array_combine_when_one_of_arrays_is_empty() : void
    {
        self::assertNull(
            combine(lit(['a', 'b', 'c']), lit([]))->eval(Row::create()),
        );
    }
}

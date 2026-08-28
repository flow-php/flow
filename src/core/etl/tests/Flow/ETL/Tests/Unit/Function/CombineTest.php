<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\combine;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;

final class CombineTest extends FlowTestCase
{
    public function test_array_combine(): void
    {
        static::assertSame(
            ['a' => 1, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c']), lit([1, 2, 3]))->eval(row(), flow_context()),
        );
    }

    public function test_array_combine_when_arrays_are_empty(): void
    {
        static::assertSame([], combine(lit([]), lit([]))->eval(row(), flow_context()));
    }

    public function test_array_combine_when_keys_are_not_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Combine function requires non-null arrays');

        combine(lit('a'), lit([1, 2, 3]))->eval(row(), flow_context());
    }

    public function test_array_combine_when_keys_are_not_unique(): void
    {
        static::assertSame(
            ['a' => 4, 'b' => 2, 'c' => 3],
            combine(lit(['a', 'b', 'c', 'a']), lit([1, 2, 3, 4]))->eval(row(), flow_context()),
        );
    }

    public function test_array_combine_when_one_of_arrays_is_empty(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Combine function requires keys and values arrays to have the same length');

        combine(lit(['a', 'b', 'c']), lit([]))->eval(row(), flow_context());
    }
}

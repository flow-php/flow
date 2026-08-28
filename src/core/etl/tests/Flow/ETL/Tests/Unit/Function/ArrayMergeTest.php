<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Function\ArrayMerge;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\json_entry;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;

final class ArrayMergeTest extends FlowTestCase
{
    public function test_array_merge_in_strict_mode(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        $context = flow_context(config());
        ref('a')->arrayMerge(ref('b'))->eval(row(int_entry('a', 1), json_entry('b', ['b' => 2])), $context);
    }

    public function test_array_merge_two_array_row_entries(): void
    {
        static::assertSame(
            ['a' => 1, 'b' => 2],
            ref('a')
                ->arrayMerge(ref('b'))
                ->eval(row(json_entry('a', ['a' => 1]), json_entry('b', ['b' => 2])), flow_context()),
        );
    }

    public function test_array_merge_two_lit_functions(): void
    {
        $function = new ArrayMerge(lit(['a' => 1]), lit(['b' => 2]));

        static::assertSame(['a' => 1, 'b' => 2], $function->eval(row(), flow_context()));
    }

    public function test_array_merge_when_left_side_is_not_an_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        ref('a')->arrayMerge(ref('b'))->eval(row(int_entry('a', 1), json_entry('b', ['b' => 2])), flow_context());
    }

    public function test_array_merge_when_right_side_is_not_an_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "array<mixed>", got "integer".');

        ref('a')->arrayMerge(ref('b'))->eval(row(json_entry('a', ['a' => 1]), int_entry('b', 2)), flow_context());
    }
}

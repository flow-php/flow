<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\bool_entry;
use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\str_entry;

final class WordwrapTest extends FlowTestCase
{
    public function test_empty_string(): void
    {
        $result = ref('str')->wordwrap(10)->eval(row(str_entry('str', '')), flow_context());

        static::assertEquals('', $result);
    }

    public function test_normal_word_wrapping(): void
    {
        $result = ref('str')->wordwrap(10)->eval(row(str_entry('str', 'The quick brown fox jumps')), flow_context());

        static::assertEquals("The quick\nbrown fox\njumps", $result);
    }

    public function test_null_break_character(): void
    {
        $result = ref('str')
            ->wordwrap(10, ref('break'))
            ->eval(row(str_entry('str', 'Hello World Test'), str_entry('break', null)), flow_context());

        static::assertEquals("Hello\nWorld Test", $result);
    }

    public function test_null_value(): void
    {
        $result = ref('str')->wordwrap(10)->eval(row(str_entry('str', null)), flow_context());

        static::assertNull($result);
    }

    public function test_text_shorter_than_width(): void
    {
        $result = ref('str')->wordwrap(20)->eval(row(str_entry('str', 'Hello')), flow_context());

        static::assertEquals('Hello', $result);
    }

    public function test_width_zero(): void
    {
        $result = ref('str')->wordwrap(0)->eval(row(str_entry('str', 'Hello World')), flow_context());

        static::assertEquals('Hello World', $result);
    }

    public function test_with_scalar_function_break(): void
    {
        $result = ref('str')
            ->wordwrap(10, ref('break'))
            ->eval(row(str_entry('str', 'Hello World Test'), str_entry('break', ' | ')), flow_context());

        static::assertEquals('Hello | World Test', $result);
    }

    public function test_with_scalar_function_cut(): void
    {
        $result = ref('str')
            ->wordwrap(3, "\n", ref('cut'))
            ->eval(row(str_entry('str', 'Hello'), bool_entry('cut', true)), flow_context());

        static::assertEquals("Hel\nlo", $result);
    }

    public function test_with_scalar_function_width(): void
    {
        $result = ref('str')
            ->wordwrap(ref('width'))
            ->eval(row(str_entry('str', 'Hello World Test'), int_entry('width', 8)), flow_context());

        static::assertEquals("Hello\nWorld\nTest", $result);
    }
}

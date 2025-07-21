<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{bool_entry, int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class WordwrapTest extends FlowTestCase
{
    public function test_empty_string() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('', $result);
    }

    public function test_normal_word_wrapping() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', 'The quick brown fox jumps'))
        );

        self::assertEquals("The quick\nbrown fox\njumps", $result);
    }

    public function test_null_break_character() : void
    {
        $result = ref('str')->wordwrap(10, ref('break'))->eval(
            row(
                str_entry('str', 'Hello World Test'),
                str_entry('break', null)
            )
        );

        self::assertEquals("Hello\nWorld Test", $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_text_shorter_than_width() : void
    {
        $result = ref('str')->wordwrap(20)->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('Hello', $result);
    }

    public function test_width_zero() : void
    {
        $result = ref('str')->wordwrap(0)->eval(
            row(str_entry('str', 'Hello World'))
        );

        self::assertEquals('Hello World', $result);
    }

    public function test_with_scalar_function_break() : void
    {
        $result = ref('str')->wordwrap(10, ref('break'))->eval(
            row(
                str_entry('str', 'Hello World Test'),
                str_entry('break', ' | ')
            )
        );

        self::assertEquals('Hello | World Test', $result);
    }

    public function test_with_scalar_function_cut() : void
    {
        $result = ref('str')->wordwrap(3, "\n", ref('cut'))->eval(
            row(
                str_entry('str', 'Hello'),
                bool_entry('cut', true)
            )
        );

        self::assertEquals("Hel\nlo", $result);
    }

    public function test_with_scalar_function_width() : void
    {
        $result = ref('str')->wordwrap(ref('width'))->eval(
            row(
                str_entry('str', 'Hello World Test'),
                int_entry('width', 8)
            )
        );

        self::assertEquals("Hello\nWorld\nTest", $result);
    }
}

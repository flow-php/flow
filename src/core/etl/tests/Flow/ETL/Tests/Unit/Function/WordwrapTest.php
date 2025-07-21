<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{bool_entry, int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class WordwrapTest extends FlowTestCase
{
    public function test_custom_line_break_character() : void
    {
        $result = ref('str')->wordwrap(10, ' | ')->eval(
            row(str_entry('str', 'The quick brown fox jumps'))
        );

        self::assertEquals('The quick | brown fox | jumps', $result);
    }

    public function test_email_addresses() : void
    {
        $result = ref('str')->wordwrap(15, "\n", false)->eval(
            row(str_entry('str', 'Contact us at support@example.com for help'))
        );

        self::assertEquals("Contact us at\nsupport@example.com\nfor help", $result);
    }

    public function test_emoji_content() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', 'Hello 👋 World 🌍 Test 🚀'))
        );

        self::assertEquals("Hello 👋\nWorld 🌍\nTest 🚀", $result);
    }

    public function test_empty_string() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('', $result);
    }

    public function test_html_content() : void
    {
        $result = ref('str')->wordwrap(15)->eval(
            row(str_entry('str', '<p>Hello World this is a test</p>'))
        );

        self::assertEquals("<p>Hello World\nthis is a\ntest</p>", $result);
    }

    public function test_json_content() : void
    {
        $result = ref('str')->wordwrap(20)->eval(
            row(str_entry('str', '{"name": "John Doe", "age": 30, "city": "New York"}'))
        );

        self::assertEquals("{\"name\": \"John Doe\",\n\"age\": 30, \"city\":\n\"New York\"}", $result);
    }

    public function test_long_words_with_cut_false() : void
    {
        $result = ref('str')->wordwrap(5, "\n", false)->eval(
            row(str_entry('str', 'Supercalifragilisticexpialidocious'))
        );

        self::assertEquals('Supercalifragilisticexpialidocious', $result);
    }

    public function test_multiple_spaces_between_words() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', 'Hello     World'))
        );

        self::assertEquals("Hello    \nWorld", $result);
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

    public function test_only_spaces() : void
    {
        $result = ref('str')->wordwrap(5)->eval(
            row(str_entry('str', '     '))
        );

        self::assertEquals('     ', $result);
    }

    public function test_paragraph_formatting() : void
    {
        $text = 'Lorem ipsum dolor sit amet, consectetur adipiscing elit. Sed do eiusmod tempor incididunt ut labore et dolore magna aliqua.';
        $result = ref('str')->wordwrap(30)->eval(
            row(str_entry('str', $text))
        );

        $expected = "Lorem ipsum dolor sit amet,\nconsectetur adipiscing elit.\nSed do eiusmod tempor\nincididunt ut labore et dolore\nmagna aliqua.";
        self::assertEquals($expected, $result);
    }

    public function test_preserves_word_boundaries() : void
    {
        $result = ref('str')->wordwrap(8)->eval(
            row(str_entry('str', 'word boundaries test'))
        );

        self::assertEquals("word\nboundaries\ntest", $result);
    }

    public function test_punctuation_handling() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', 'Hello, world! How are you?'))
        );

        self::assertEquals("Hello,\nworld! How\nare you?", $result);
    }

    public function test_report_formatting() : void
    {
        $result = ref('str')->wordwrap(25)->eval(
            row(str_entry('str', 'Sales Report: Total revenue for Q1 was $50,000 with growth of 15%'))
        );

        self::assertEquals("Sales Report: Total\nrevenue for Q1 was\n$50,000 with growth of\n15%", $result);
    }

    public function test_single_long_word_no_cut() : void
    {
        $result = ref('str')->wordwrap(5, "\n", false)->eval(
            row(str_entry('str', 'antidisestablishmentarianism'))
        );

        self::assertEquals('antidisestablishmentarianism', $result);
    }

    public function test_text_shorter_than_width() : void
    {
        $result = ref('str')->wordwrap(20)->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('Hello', $result);
    }

    public function test_unicode_strings() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', 'नमस्ते दोस्त कैसे हैं आप'))
        );

        self::assertEquals("नमस्ते दोस्त\nकैसे हैं आप", $result);
    }

    public function test_urls() : void
    {
        $result = ref('str')->wordwrap(20, "\n", false)->eval(
            row(str_entry('str', 'Visit https://www.example.com/very/long/path'))
        );

        self::assertEquals("Visit\nhttps://www.example.com/very/long/path", $result);
    }

    public function test_very_large_width() : void
    {
        $result = ref('str')->wordwrap(1000)->eval(
            row(str_entry('str', 'Hello World this is a test'))
        );

        self::assertEquals('Hello World this is a test', $result);
    }

    public function test_width_one() : void
    {
        $result = ref('str')->wordwrap(1, "\n", true)->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals("H\ne\nl\nl\no", $result);
    }

    public function test_width_one_no_cut() : void
    {
        $result = ref('str')->wordwrap(1, "\n", false)->eval(
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

    public function test_with_existing_line_breaks() : void
    {
        $result = ref('str')->wordwrap(10)->eval(
            row(str_entry('str', "Hello\nWorld\nThis is a test"))
        );

        self::assertEquals("Hello\nWorld\nThis is a\ntest", $result);
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

    public function test_words_exactly_at_width() : void
    {
        $result = ref('str')->wordwrap(5)->eval(
            row(str_entry('str', 'Hello World'))
        );

        self::assertEquals("Hello\nWorld", $result);
    }
}

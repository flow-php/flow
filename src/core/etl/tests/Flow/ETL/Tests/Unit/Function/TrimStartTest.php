<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class TrimStartTest extends FlowTestCase
{
    public function test_trim_start_case_sensitive() : void
    {
        $result = ref('str')->trimStart('abc')->eval(
            row(str_entry('str', 'ABCabcHello'))
        );

        self::assertEquals('ABCabcHello', $result);
    }

    public function test_trim_start_csv_data() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   Name,Age,City'))
        );

        self::assertEquals('Name,Age,City', $result);
    }

    public function test_trim_start_currency_symbols() : void
    {
        $result = ref('str')->trimStart('$')->eval(
            row(str_entry('str', '$$$123.45'))
        );

        self::assertEquals('123.45', $result);
    }

    public function test_trim_start_custom_characters() : void
    {
        $result = ref('str')->trimStart('abc')->eval(
            row(str_entry('str', 'abcabcHello world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_trim_start_custom_characters_mixed() : void
    {
        $result = ref('str')->trimStart('.,!')->eval(
            row(str_entry('str', '...!!!Text with punctuation'))
        );

        self::assertEquals('Text with punctuation', $result);
    }

    public function test_trim_start_data_cleaning_scenario() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   John Doe   '))
        );

        self::assertEquals('John Doe   ', $result);
    }

    public function test_trim_start_default_whitespace() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   Hello world   '))
        );

        self::assertEquals('Hello world   ', $result);
    }

    public function test_trim_start_email_prefixes() : void
    {
        $result = ref('str')->trimStart('mailto:')->eval(
            row(str_entry('str', 'mailto:user@example.com'))
        );

        self::assertEquals('user@example.com', $result);
    }

    public function test_trim_start_empty_string() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('', $result);
    }

    public function test_trim_start_file_paths() : void
    {
        $result = ref('str')->trimStart('/')->eval(
            row(str_entry('str', '///path/to/file'))
        );

        self::assertEquals('path/to/file', $result);
    }

    public function test_trim_start_html_content() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   <p>HTML content</p>'))
        );

        self::assertEquals('<p>HTML content</p>', $result);
    }

    public function test_trim_start_json_content() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   {"key": "value"}'))
        );

        self::assertEquals('{"key": "value"}', $result);
    }

    public function test_trim_start_leading_spaces() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '     Text with leading spaces'))
        );

        self::assertEquals('Text with leading spaces', $result);
    }

    public function test_trim_start_log_prefixes() : void
    {
        $result = ref('str')->trimStart('[ERROR]')->eval(
            row(str_entry('str', '[ERROR][ERROR]Database connection failed'))
        );

        self::assertEquals('Database connection failed', $result);
    }

    public function test_trim_start_mixed_whitespace() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', " \t\n\r Text with all whitespace types"))
        );

        self::assertEquals('Text with all whitespace types', $result);
    }

    public function test_trim_start_multiple_same_characters() : void
    {
        $result = ref('str')->trimStart('x')->eval(
            row(str_entry('str', 'xxxxxxHello world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_trim_start_newlines() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', "\n\n\nText with newlines"))
        );

        self::assertEquals('Text with newlines', $result);
    }

    public function test_trim_start_no_leading_characters() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', 'No leading whitespace   '))
        );

        self::assertEquals('No leading whitespace   ', $result);
    }

    public function test_trim_start_null_value() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_trim_start_number_strings() : void
    {
        $result = ref('str')->trimStart('0')->eval(
            row(str_entry('str', '00012345'))
        );

        self::assertEquals('12345', $result);
    }

    public function test_trim_start_only_leading_characters() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   \t\n   '))
        );

        self::assertEquals('', $result);
    }

    public function test_trim_start_only_removes_leading() : void
    {
        $result = ref('str')->trimStart('abc')->eval(
            row(str_entry('str', 'abcHello worldabc'))
        );

        self::assertEquals('Hello worldabc', $result);
    }

    public function test_trim_start_preserves_trailing_whitespace() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   Text with trailing   '))
        );

        self::assertEquals('Text with trailing   ', $result);
    }

    public function test_trim_start_single_character() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', ' X'))
        );

        self::assertEquals('X', $result);
    }

    public function test_trim_start_special_characters() : void
    {
        $result = ref('str')->trimStart('@#$')->eval(
            row(str_entry('str', '@#$@#$Hello@#$'))
        );

        self::assertEquals('Hello@#$', $result);
    }

    public function test_trim_start_tabs_and_spaces() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', "\t\t   Mixed tabs and spaces"))
        );

        self::assertEquals('Mixed tabs and spaces', $result);
    }

    public function test_trim_start_unicode_characters() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '  नमस्ते दुनिया  '))
        );

        self::assertEquals('नमस्ते दुनिया  ', $result);
    }

    public function test_trim_start_unicode_whitespace() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', " \u{00A0}\u{2000}\u{2001}Unicode text"))
        );

        self::assertEquals('Unicode text', $result);
    }

    public function test_trim_start_url_prefixes() : void
    {
        $result = ref('str')->trimStart('http://')->eval(
            row(str_entry('str', 'http://example.com'))
        );

        self::assertEquals('example.com', $result);
    }

    public function test_trim_start_with_scalar_function_chars() : void
    {
        $result = ref('str')->trimStart(ref('chars'))->eval(
            row(
                str_entry('str', '###Text with hashes'),
                str_entry('chars', '#')
            )
        );

        self::assertEquals('Text with hashes', $result);
    }

    public function test_trim_start_xml_content() : void
    {
        $result = ref('str')->trimStart()->eval(
            row(str_entry('str', '   <?xml version="1.0"?>'))
        );

        self::assertEquals('<?xml version="1.0"?>', $result);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class TrimEndTest extends FlowTestCase
{
    public function test_trim_end_brackets() : void
    {
        $result = ref('str')->trimEnd(']')->eval(
            row(str_entry('str', 'Array access[index]]]'))
        );

        self::assertEquals('Array access[index', $result);
    }

    public function test_trim_end_case_sensitive() : void
    {
        $result = ref('str')->trimEnd('abc')->eval(
            row(str_entry('str', 'HelloABCabc'))
        );

        self::assertEquals('HelloABC', $result);
    }

    public function test_trim_end_commas() : void
    {
        $result = ref('str')->trimEnd(',')->eval(
            row(str_entry('str', 'List item,,,'))
        );

        self::assertEquals('List item', $result);
    }

    public function test_trim_end_csv_data() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', 'Name,Age,City   '))
        );

        self::assertEquals('Name,Age,City', $result);
    }

    public function test_trim_end_currency_symbols() : void
    {
        $result = ref('str')->trimEnd('$')->eval(
            row(str_entry('str', '123.45$$$'))
        );

        self::assertEquals('123.45', $result);
    }

    public function test_trim_end_custom_characters() : void
    {
        $result = ref('str')->trimEnd('abc')->eval(
            row(str_entry('str', 'Hello worldabcabc'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_trim_end_custom_characters_mixed() : void
    {
        $result = ref('str')->trimEnd('.,!')->eval(
            row(str_entry('str', 'Text with punctuation...!!!'))
        );

        self::assertEquals('Text with punctuation', $result);
    }

    public function test_trim_end_data_cleaning_scenario() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '   John Doe   '))
        );

        self::assertEquals('   John Doe', $result);
    }

    public function test_trim_end_default_whitespace() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '   Hello world   '))
        );

        self::assertEquals('   Hello world', $result);
    }

    public function test_trim_end_dots() : void
    {
        $result = ref('str')->trimEnd('.')->eval(
            row(str_entry('str', 'End of sentence...'))
        );

        self::assertEquals('End of sentence', $result);
    }

    public function test_trim_end_empty_string() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('', $result);
    }

    public function test_trim_end_file_extensions() : void
    {
        $result = ref('str')->trimEnd('.txt')->eval(
            row(str_entry('str', 'document.txt.txt'))
        );

        self::assertEquals('document', $result);
    }

    public function test_trim_end_html_content() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '<p>HTML content</p>   '))
        );

        self::assertEquals('<p>HTML content</p>', $result);
    }

    public function test_trim_end_json_content() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '{"key": "value"}   '))
        );

        self::assertEquals('{"key": "value"}', $result);
    }

    public function test_trim_end_log_suffixes() : void
    {
        $result = ref('str')->trimEnd('[/ERROR]')->eval(
            row(str_entry('str', 'Database connection failed[/ERROR][/ERROR]'))
        );

        self::assertEquals('Database connection failed', $result);
    }

    public function test_trim_end_mixed_punctuation() : void
    {
        $result = ref('str')->trimEnd('!?.,')->eval(
            row(str_entry('str', 'Sentence with mixed punctuation!?.,'))
        );

        self::assertEquals('Sentence with mixed punctuation', $result);
    }

    public function test_trim_end_mixed_whitespace() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', "Text with all whitespace types \t\n\r"))
        );

        self::assertEquals('Text with all whitespace types', $result);
    }

    public function test_trim_end_multiple_same_characters() : void
    {
        $result = ref('str')->trimEnd('x')->eval(
            row(str_entry('str', 'Hello worldxxxxxx'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_trim_end_newlines() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', "Text with newlines\n\n\n"))
        );

        self::assertEquals('Text with newlines', $result);
    }

    public function test_trim_end_no_trailing_characters() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '   No trailing whitespace'))
        );

        self::assertEquals('   No trailing whitespace', $result);
    }

    public function test_trim_end_null_value() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_trim_end_number_strings() : void
    {
        $result = ref('str')->trimEnd('0')->eval(
            row(str_entry('str', '12345000'))
        );

        self::assertEquals('12345', $result);
    }

    public function test_trim_end_only_removes_trailing() : void
    {
        $result = ref('str')->trimEnd('abc')->eval(
            row(str_entry('str', 'abcHello worldabc'))
        );

        self::assertEquals('abcHello world', $result);
    }

    public function test_trim_end_only_trailing_characters() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '   \t\n   '))
        );

        self::assertEquals('', $result);
    }

    public function test_trim_end_parentheses() : void
    {
        $result = ref('str')->trimEnd(')')->eval(
            row(str_entry('str', 'Function call())'))
        );

        self::assertEquals('Function call(', $result);
    }

    public function test_trim_end_preserves_leading_whitespace() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '   Text with leading   '))
        );

        self::assertEquals('   Text with leading', $result);
    }

    public function test_trim_end_quotes() : void
    {
        $result = ref('str')->trimEnd('"')->eval(
            row(str_entry('str', 'Quoted text"""'))
        );

        self::assertEquals('Quoted text', $result);
    }

    public function test_trim_end_single_character() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', 'X '))
        );

        self::assertEquals('X', $result);
    }

    public function test_trim_end_special_characters() : void
    {
        $result = ref('str')->trimEnd('@#$')->eval(
            row(str_entry('str', '@#$Hello@#$@#$'))
        );

        self::assertEquals('@#$Hello', $result);
    }

    public function test_trim_end_sql_semicolons() : void
    {
        $result = ref('str')->trimEnd(';')->eval(
            row(str_entry('str', 'SELECT * FROM users;;;'))
        );

        self::assertEquals('SELECT * FROM users', $result);
    }

    public function test_trim_end_tabs_and_spaces() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', "Mixed tabs and spaces\t\t   "))
        );

        self::assertEquals('Mixed tabs and spaces', $result);
    }

    public function test_trim_end_trailing_spaces() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', 'Text with trailing spaces     '))
        );

        self::assertEquals('Text with trailing spaces', $result);
    }

    public function test_trim_end_unicode_characters() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '  नमस्ते दुनिया  '))
        );

        self::assertEquals('  नमस्ते दुनिया', $result);
    }

    public function test_trim_end_unicode_whitespace() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', "Unicode text \u{00A0}\u{2000}\u{2001}"))
        );

        self::assertEquals('Unicode text', $result);
    }

    public function test_trim_end_url_suffixes() : void
    {
        $result = ref('str')->trimEnd('/')->eval(
            row(str_entry('str', 'example.com///'))
        );

        self::assertEquals('example.com', $result);
    }

    public function test_trim_end_with_scalar_function_chars() : void
    {
        $result = ref('str')->trimEnd(ref('chars'))->eval(
            row(
                str_entry('str', 'Text with hashes###'),
                str_entry('chars', '#')
            )
        );

        self::assertEquals('Text with hashes', $result);
    }

    public function test_trim_end_xml_content() : void
    {
        $result = ref('str')->trimEnd()->eval(
            row(str_entry('str', '<?xml version="1.0"?>   '))
        );

        self::assertEquals('<?xml version="1.0"?>', $result);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class EnsureEndTest extends FlowTestCase
{
    public function test_case_sensitivity() : void
    {
        $result = ref('str')->ensureEnd('.TXT')->eval(
            row(str_entry('str', 'document.txt'))
        );

        self::assertEquals('document.txt.TXT', $result);
    }

    public function test_css_declaration_semicolon() : void
    {
        $result = ref('str')->ensureEnd(';')->eval(
            row(str_entry('str', 'color: red'))
        );

        self::assertEquals('color: red;', $result);
    }

    public function test_css_declaration_semicolon_already_present() : void
    {
        $result = ref('str')->ensureEnd(';')->eval(
            row(str_entry('str', 'color: red;'))
        );

        self::assertEquals('color: red;', $result);
    }

    public function test_empty_string_with_suffix() : void
    {
        $result = ref('str')->ensureEnd('_suffix')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('_suffix', $result);
    }

    public function test_file_extension_already_present() : void
    {
        $result = ref('str')->ensureEnd('.log')->eval(
            row(str_entry('str', 'app.log'))
        );

        self::assertEquals('app.log', $result);
    }

    public function test_file_extension_normalization() : void
    {
        $result = ref('str')->ensureEnd('.log')->eval(
            row(str_entry('str', 'app'))
        );

        self::assertEquals('app.log', $result);
    }

    public function test_html_paragraph_closing() : void
    {
        $result = ref('str')->ensureEnd('</p>')->eval(
            row(str_entry('str', '<p>Hello world'))
        );

        self::assertEquals('<p>Hello world</p>', $result);
    }

    public function test_html_paragraph_closing_already_present() : void
    {
        $result = ref('str')->ensureEnd('</p>')->eval(
            row(str_entry('str', '<p>Hello world</p>'))
        );

        self::assertEquals('<p>Hello world</p>', $result);
    }

    public function test_json_property_comma() : void
    {
        $result = ref('str')->ensureEnd(',')->eval(
            row(str_entry('str', '"name": "John"'))
        );

        self::assertEquals('"name": "John",', $result);
    }

    public function test_json_property_comma_already_present() : void
    {
        $result = ref('str')->ensureEnd(',')->eval(
            row(str_entry('str', '"name": "John",'))
        );

        self::assertEquals('"name": "John",', $result);
    }

    public function test_large_strings() : void
    {
        $largeString = str_repeat('a', 1000);
        $suffix = str_repeat('b', 500);

        $result = ref('str')->ensureEnd($suffix)->eval(
            row(str_entry('str', $largeString))
        );

        self::assertEquals($largeString . $suffix, $result);
    }

    public function test_multiple_application() : void
    {
        $result = ref('str')->ensureEnd('_suffix')->ensureEnd('_suffix')->eval(
            row(str_entry('str', 'content'))
        );

        self::assertEquals('content_suffix', $result);
    }

    public function test_multiple_chaining() : void
    {
        $result = ref('str')->ensureEnd(' ')->ensureEnd('world')->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_null_suffix() : void
    {
        $result = ref('str')->ensureEnd(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', null)
            )
        );

        self::assertEquals('hello', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->ensureEnd('_suffix')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_numbers_as_strings() : void
    {
        $result = ref('str')->ensureEnd('00')->eval(
            row(str_entry('str', '123'))
        );

        self::assertEquals('12300', $result);
    }

    public function test_partial_suffix_match() : void
    {
        $result = ref('str')->ensureEnd(' world')->eval(
            row(str_entry('str', 'hello wor'))
        );

        self::assertEquals('hello wor world', $result);
    }

    public function test_sql_query_semicolon() : void
    {
        $result = ref('str')->ensureEnd(';')->eval(
            row(str_entry('str', 'SELECT * FROM users'))
        );

        self::assertEquals('SELECT * FROM users;', $result);
    }

    public function test_sql_query_semicolon_already_present() : void
    {
        $result = ref('str')->ensureEnd(';')->eval(
            row(str_entry('str', 'SELECT * FROM users;'))
        );

        self::assertEquals('SELECT * FROM users;', $result);
    }

    public function test_string_already_ends_with_suffix() : void
    {
        $result = ref('str')->ensureEnd('.txt')->eval(
            row(str_entry('str', 'document.txt'))
        );

        self::assertEquals('document.txt', $result);
    }

    public function test_string_doesnt_end_with_suffix() : void
    {
        $result = ref('str')->ensureEnd('.txt')->eval(
            row(str_entry('str', 'document'))
        );

        self::assertEquals('document.txt', $result);
    }

    public function test_string_with_empty_suffix() : void
    {
        $result = ref('str')->ensureEnd('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_unicode_already_ends_with_suffix() : void
    {
        $result = ref('str')->ensureEnd(' जी')->eval(
            row(str_entry('str', 'नमस्ते जी'))
        );

        self::assertEquals('नमस्ते जी', $result);
    }

    public function test_unicode_strings() : void
    {
        $result = ref('str')->ensureEnd(' जी')->eval(
            row(str_entry('str', 'नमस्ते'))
        );

        self::assertEquals('नमस्ते जी', $result);
    }

    public function test_url_path_already_present() : void
    {
        $result = ref('str')->ensureEnd('/')->eval(
            row(str_entry('str', 'https://example.com/api/'))
        );

        self::assertEquals('https://example.com/api/', $result);
    }

    public function test_url_path_normalization() : void
    {
        $result = ref('str')->ensureEnd('/')->eval(
            row(str_entry('str', 'https://example.com/api'))
        );

        self::assertEquals('https://example.com/api/', $result);
    }

    public function test_with_emoji_suffix() : void
    {
        $result = ref('str')->ensureEnd(' 🎉')->eval(
            row(str_entry('str', 'Success'))
        );

        self::assertEquals('Success 🎉', $result);
    }

    public function test_with_emoji_suffix_already_present() : void
    {
        $result = ref('str')->ensureEnd(' 🎉')->eval(
            row(str_entry('str', 'Success 🎉'))
        );

        self::assertEquals('Success 🎉', $result);
    }

    public function test_with_newline_suffix() : void
    {
        $result = ref('str')->ensureEnd("\n")->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals("hello\n", $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->ensureEnd(ref('suffix'))->eval(
            row(
                str_entry('str', 'document'),
                str_entry('suffix', '.pdf')
            )
        );

        self::assertEquals('document.pdf', $result);
    }

    public function test_with_special_characters() : void
    {
        $result = ref('str')->ensureEnd('$#@')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello$#@', $result);
    }

    public function test_with_special_characters_already_present() : void
    {
        $result = ref('str')->ensureEnd('$#@')->eval(
            row(str_entry('str', 'hello$#@'))
        );

        self::assertEquals('hello$#@', $result);
    }

    public function test_with_tab_suffix() : void
    {
        $result = ref('str')->ensureEnd("\t")->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals("hello\t", $result);
    }

    public function test_with_whitespace_suffix() : void
    {
        $result = ref('str')->ensureEnd('  ')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello  ', $result);
    }

    public function test_with_whitespace_suffix_already_present() : void
    {
        $result = ref('str')->ensureEnd('  ')->eval(
            row(str_entry('str', 'hello  '))
        );

        self::assertEquals('hello  ', $result);
    }

    public function test_xml_tag_closure() : void
    {
        $result = ref('str')->ensureEnd('/>')->eval(
            row(str_entry('str', '<img src="image.png"'))
        );

        self::assertEquals('<img src="image.png"/>', $result);
    }

    public function test_xml_tag_closure_already_present() : void
    {
        $result = ref('str')->ensureEnd('/>')->eval(
            row(str_entry('str', '<img src="image.png"/>'))
        );

        self::assertEquals('<img src="image.png"/>', $result);
    }
}

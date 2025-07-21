<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class AppendTest extends FlowTestCase
{
    public function test_append_empty_string_to_content() : void
    {
        $result = ref('str')->append('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_file_extensions() : void
    {
        $result = ref('str')->append('.txt')->eval(
            row(str_entry('str', 'filename'))
        );

        self::assertEquals('filename.txt', $result);
    }

    public function test_append_html_content() : void
    {
        $result = ref('str')->append('<span>world</span>')->eval(
            row(str_entry('str', '<div>hello</div>'))
        );

        self::assertEquals('<div>hello</div><span>world</span>', $result);
    }

    public function test_append_json_content() : void
    {
        $result = ref('str')->append(',"key2":"value2"}')->eval(
            row(str_entry('str', '{"key1":"value1"'))
        );

        self::assertEquals('{"key1":"value1","key2":"value2"}', $result);
    }

    public function test_append_large_strings() : void
    {
        $largeString = str_repeat('a', 1000);
        $suffix = str_repeat('b', 500);

        $result = ref('str')->append($suffix)->eval(
            row(str_entry('str', $largeString))
        );

        self::assertEquals($largeString . $suffix, $result);
    }

    public function test_append_multiple_chaining() : void
    {
        $result = ref('str')->append(' ')->append('world')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello world', $result);
    }

    public function test_append_numbers_as_strings() : void
    {
        $result = ref('str')->append('123')->eval(
            row(str_entry('str', 'value'))
        );

        self::assertEquals('value123', $result);
    }

    public function test_append_to_empty_string() : void
    {
        $result = ref('str')->append('hello')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_to_non_empty_string() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello world', $result);
    }

    public function test_append_unicode_strings() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', 'नमस्ते'))
        );

        self::assertEquals('नमस्ते world', $result);
    }

    public function test_append_unicode_to_unicode() : void
    {
        $result = ref('str')->append(' स्वागत')->eval(
            row(str_entry('str', 'नमस्ते'))
        );

        self::assertEquals('नमस्ते स्वागत', $result);
    }

    public function test_append_url_parameters() : void
    {
        $result = ref('str')->append('?param=value')->eval(
            row(str_entry('str', 'https://example.com'))
        );

        self::assertEquals('https://example.com?param=value', $result);
    }

    public function test_append_with_newlines() : void
    {
        $result = ref('str')->append("\nworld")->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals("hello\nworld", $result);
    }

    public function test_append_with_null_suffix() : void
    {
        $result = ref('str')->append(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', null)
            )
        );

        self::assertEquals('hello', $result);
    }

    public function test_append_with_null_value() : void
    {
        $result = ref('str')->append(' world')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_append_with_scalar_function_parameter() : void
    {
        $result = ref('str')->append(ref('suffix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('suffix', ' world')
            )
        );

        self::assertEquals('hello world', $result);
    }

    public function test_append_with_spaces() : void
    {
        $result = ref('str')->append('   suffix   ')->eval(
            row(str_entry('str', 'prefix'))
        );

        self::assertEquals('prefix   suffix   ', $result);
    }

    public function test_append_with_special_characters() : void
    {
        $result = ref('str')->append('@#$%^&*()')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello@#$%^&*()', $result);
    }

    public function test_append_with_tabs() : void
    {
        $result = ref('str')->append("\tworld")->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals("hello\tworld", $result);
    }

    public function test_append_with_unicode_characters() : void
    {
        $result = ref('str')->append(' 🌍')->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('Hello 🌍', $result);
    }
}

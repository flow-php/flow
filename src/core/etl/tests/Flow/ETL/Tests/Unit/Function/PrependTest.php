<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class PrependTest extends FlowTestCase
{
    public function test_prepend_command_prefix() : void
    {
        $result = ref('str')->prepend('sudo ')->eval(
            row(str_entry('str', 'systemctl restart nginx'))
        );

        self::assertEquals('sudo systemctl restart nginx', $result);
    }

    public function test_prepend_empty_string_to_content() : void
    {
        $result = ref('str')->prepend('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_prepend_html_content() : void
    {
        $result = ref('str')->prepend('<div>hello</div>')->eval(
            row(str_entry('str', '<span>world</span>'))
        );

        self::assertEquals('<div>hello</div><span>world</span>', $result);
    }

    public function test_prepend_json_content() : void
    {
        $result = ref('str')->prepend('{"key1":"value1",')->eval(
            row(str_entry('str', '"key2":"value2"}'))
        );

        self::assertEquals('{"key1":"value1","key2":"value2"}', $result);
    }

    public function test_prepend_large_strings() : void
    {
        $largeString = str_repeat('a', 1000);
        $prefix = str_repeat('b', 500);

        $result = ref('str')->prepend($prefix)->eval(
            row(str_entry('str', $largeString))
        );

        self::assertEquals($prefix . $largeString, $result);
    }

    public function test_prepend_multiple_chaining() : void
    {
        $result = ref('str')->prepend(' ')->prepend('Hello')->eval(
            row(str_entry('str', 'world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_prepend_numbers_as_strings() : void
    {
        $result = ref('str')->prepend('123')->eval(
            row(str_entry('str', 'value'))
        );

        self::assertEquals('123value', $result);
    }

    public function test_prepend_path_prefix() : void
    {
        $result = ref('str')->prepend('/var/log/')->eval(
            row(str_entry('str', 'app.log'))
        );

        self::assertEquals('/var/log/app.log', $result);
    }

    public function test_prepend_protocol_to_url() : void
    {
        $result = ref('str')->prepend('https://')->eval(
            row(str_entry('str', 'example.com'))
        );

        self::assertEquals('https://example.com', $result);
    }

    public function test_prepend_sql_prefix() : void
    {
        $result = ref('str')->prepend('SELECT * FROM ')->eval(
            row(str_entry('str', 'users WHERE active = 1'))
        );

        self::assertEquals('SELECT * FROM users WHERE active = 1', $result);
    }

    public function test_prepend_to_empty_string() : void
    {
        $result = ref('str')->prepend('hello')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('hello', $result);
    }

    public function test_prepend_to_non_empty_string() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', 'world'))
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_prepend_unicode_strings() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', 'नमस्ते'))
        );

        self::assertEquals('Hello नमस्ते', $result);
    }

    public function test_prepend_unicode_to_unicode() : void
    {
        $result = ref('str')->prepend('स्वागत ')->eval(
            row(str_entry('str', 'नमस्ते'))
        );

        self::assertEquals('स्वागत नमस्ते', $result);
    }

    public function test_prepend_with_newlines() : void
    {
        $result = ref('str')->prepend("prefix\n")->eval(
            row(str_entry('str', 'world'))
        );

        self::assertEquals("prefix\nworld", $result);
    }

    public function test_prepend_with_null_prefix() : void
    {
        $result = ref('str')->prepend(ref('prefix'))->eval(
            row(
                str_entry('str', 'world'),
                str_entry('prefix', null)
            )
        );

        self::assertEquals('world', $result);
    }

    public function test_prepend_with_null_value() : void
    {
        $result = ref('str')->prepend('Hello ')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_prepend_with_scalar_function_parameter() : void
    {
        $result = ref('str')->prepend(ref('prefix'))->eval(
            row(
                str_entry('str', 'world'),
                str_entry('prefix', 'Hello ')
            )
        );

        self::assertEquals('Hello world', $result);
    }

    public function test_prepend_with_spaces() : void
    {
        $result = ref('str')->prepend('   prefix   ')->eval(
            row(str_entry('str', 'suffix'))
        );

        self::assertEquals('   prefix   suffix', $result);
    }

    public function test_prepend_with_special_characters() : void
    {
        $result = ref('str')->prepend('@#$%^&*()')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('@#$%^&*()hello', $result);
    }

    public function test_prepend_with_tabs() : void
    {
        $result = ref('str')->prepend("prefix\t")->eval(
            row(str_entry('str', 'world'))
        );

        self::assertEquals("prefix\tworld", $result);
    }

    public function test_prepend_with_unicode_characters() : void
    {
        $result = ref('str')->prepend('🌍 ')->eval(
            row(str_entry('str', 'Hello'))
        );

        self::assertEquals('🌍 Hello', $result);
    }
}

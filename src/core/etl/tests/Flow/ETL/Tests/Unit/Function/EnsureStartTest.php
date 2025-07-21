<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class EnsureStartTest extends FlowTestCase
{
    public function test_case_sensitivity() : void
    {
        $result = ref('str')->ensureStart('HTTP://')->eval(
            row(str_entry('str', 'https://example.com'))
        );

        self::assertEquals('HTTP://https://example.com', $result);
    }

    public function test_command_prefix() : void
    {
        $result = ref('str')->ensureStart('sudo ')->eval(
            row(str_entry('str', 'systemctl restart nginx'))
        );

        self::assertEquals('sudo systemctl restart nginx', $result);
    }

    public function test_command_prefix_already_present() : void
    {
        $result = ref('str')->ensureStart('sudo ')->eval(
            row(str_entry('str', 'sudo systemctl restart nginx'))
        );

        self::assertEquals('sudo systemctl restart nginx', $result);
    }

    public function test_css_class_prefix() : void
    {
        $result = ref('str')->ensureStart('.')->eval(
            row(str_entry('str', 'button-primary'))
        );

        self::assertEquals('.button-primary', $result);
    }

    public function test_css_class_prefix_already_present() : void
    {
        $result = ref('str')->ensureStart('.')->eval(
            row(str_entry('str', '.button-primary'))
        );

        self::assertEquals('.button-primary', $result);
    }

    public function test_empty_string_with_prefix() : void
    {
        $result = ref('str')->ensureStart('prefix_')->eval(
            row(str_entry('str', ''))
        );

        self::assertEquals('prefix_', $result);
    }

    public function test_file_path_already_normalized() : void
    {
        $result = ref('str')->ensureStart('/')->eval(
            row(str_entry('str', '/var/log/app.log'))
        );

        self::assertEquals('/var/log/app.log', $result);
    }

    public function test_file_path_normalization() : void
    {
        $result = ref('str')->ensureStart('/')->eval(
            row(str_entry('str', 'var/log/app.log'))
        );

        self::assertEquals('/var/log/app.log', $result);
    }

    public function test_id_prefix() : void
    {
        $result = ref('str')->ensureStart('#')->eval(
            row(str_entry('str', 'main-content'))
        );

        self::assertEquals('#main-content', $result);
    }

    public function test_id_prefix_already_present() : void
    {
        $result = ref('str')->ensureStart('#')->eval(
            row(str_entry('str', '#main-content'))
        );

        self::assertEquals('#main-content', $result);
    }

    public function test_multiple_application() : void
    {
        $result = ref('str')->ensureStart('prefix_')->ensureStart('prefix_')->eval(
            row(str_entry('str', 'content'))
        );

        self::assertEquals('prefix_content', $result);
    }

    public function test_null_prefix() : void
    {
        $result = ref('str')->ensureStart(ref('prefix'))->eval(
            row(
                str_entry('str', 'hello'),
                str_entry('prefix', null)
            )
        );

        self::assertEquals('hello', $result);
    }

    public function test_null_value() : void
    {
        $result = ref('str')->ensureStart('prefix_')->eval(
            row(str_entry('str', null))
        );

        self::assertNull($result);
    }

    public function test_partial_prefix_match() : void
    {
        $result = ref('str')->ensureStart('hello ')->eval(
            row(str_entry('str', 'hell world'))
        );

        self::assertEquals('hello hell world', $result);
    }

    public function test_protocol_already_present() : void
    {
        $result = ref('str')->ensureStart('ftp://')->eval(
            row(str_entry('str', 'ftp://files.example.com'))
        );

        self::assertEquals('ftp://files.example.com', $result);
    }

    public function test_protocol_normalization() : void
    {
        $result = ref('str')->ensureStart('ftp://')->eval(
            row(str_entry('str', 'files.example.com'))
        );

        self::assertEquals('ftp://files.example.com', $result);
    }

    public function test_string_already_starts_with_prefix() : void
    {
        $result = ref('str')->ensureStart('https://')->eval(
            row(str_entry('str', 'https://example.com'))
        );

        self::assertEquals('https://example.com', $result);
    }

    public function test_string_doesnt_start_with_prefix() : void
    {
        $result = ref('str')->ensureStart('https://')->eval(
            row(str_entry('str', 'example.com'))
        );

        self::assertEquals('https://example.com', $result);
    }

    public function test_string_with_empty_prefix() : void
    {
        $result = ref('str')->ensureStart('')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('hello', $result);
    }

    public function test_unicode_already_starts_with_prefix() : void
    {
        $result = ref('str')->ensureStart('नमस्ते ')->eval(
            row(str_entry('str', 'नमस्ते world'))
        );

        self::assertEquals('नमस्ते world', $result);
    }

    public function test_unicode_strings() : void
    {
        $result = ref('str')->ensureStart('नमस्ते ')->eval(
            row(str_entry('str', 'world'))
        );

        self::assertEquals('नमस्ते world', $result);
    }

    public function test_with_emoji_prefix() : void
    {
        $result = ref('str')->ensureStart('🚀 ')->eval(
            row(str_entry('str', 'Launch successful'))
        );

        self::assertEquals('🚀 Launch successful', $result);
    }

    public function test_with_emoji_prefix_already_present() : void
    {
        $result = ref('str')->ensureStart('🚀 ')->eval(
            row(str_entry('str', '🚀 Launch successful'))
        );

        self::assertEquals('🚀 Launch successful', $result);
    }

    public function test_with_newline_prefix() : void
    {
        $result = ref('str')->ensureStart("\n")->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals("\nhello", $result);
    }

    public function test_with_scalar_function_parameter() : void
    {
        $result = ref('str')->ensureStart(ref('prefix'))->eval(
            row(
                str_entry('str', 'example.com'),
                str_entry('prefix', 'https://')
            )
        );

        self::assertEquals('https://example.com', $result);
    }

    public function test_with_special_characters() : void
    {
        $result = ref('str')->ensureStart('@#$')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('@#$hello', $result);
    }

    public function test_with_special_characters_already_present() : void
    {
        $result = ref('str')->ensureStart('@#$')->eval(
            row(str_entry('str', '@#$hello'))
        );

        self::assertEquals('@#$hello', $result);
    }

    public function test_with_tab_prefix() : void
    {
        $result = ref('str')->ensureStart("\t")->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals("\thello", $result);
    }

    public function test_with_whitespace_prefix() : void
    {
        $result = ref('str')->ensureStart('  ')->eval(
            row(str_entry('str', 'hello'))
        );

        self::assertEquals('  hello', $result);
    }

    public function test_with_whitespace_prefix_already_present() : void
    {
        $result = ref('str')->ensureStart('  ')->eval(
            row(str_entry('str', '  hello'))
        );

        self::assertEquals('  hello', $result);
    }
}

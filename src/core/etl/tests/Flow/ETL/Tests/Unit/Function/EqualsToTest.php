<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class EqualsToTest extends FlowTestCase
{
    public function test_equals_to_case_sensitive() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('Hello')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_emoji_characters() : void
    {
        self::assertTrue(
            ref('str')->equalsTo('🚀🌟')->eval(
                row(str_entry('str', '🚀🌟'))
            )
        );
    }

    public function test_equals_to_empty_strings() : void
    {
        self::assertTrue(
            ref('str')->equalsTo('')->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_equals_to_empty_vs_non_empty() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_exact_match() : void
    {
        self::assertTrue(
            ref('str')->equalsTo('hello')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_longer_string_returns_false() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('hello world')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_newline_and_tab_characters() : void
    {
        self::assertTrue(
            ref('str')->equalsTo("hello\nworld\t")->eval(
                row(str_entry('str', "hello\nworld\t"))
            )
        );
    }

    public function test_equals_to_no_match() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('world')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_null_comparison_string_returns_null() : void
    {
        self::assertNull(
            ref('str')->equalsTo(ref('compare'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('compare', null)
                )
            )
        );
    }

    public function test_equals_to_null_string_returns_null() : void
    {
        self::assertNull(
            ref('str')->equalsTo('hello')->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_equals_to_numbers_as_strings() : void
    {
        self::assertTrue(
            ref('str')->equalsTo('123')->eval(
                row(str_entry('str', '123'))
            )
        );
    }

    public function test_equals_to_partial_match_returns_false() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('hell')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_special_characters() : void
    {
        self::assertTrue(
            ref('str')->equalsTo('!@#$%^&*()')->eval(
                row(str_entry('str', '!@#$%^&*()'))
            )
        );
    }

    public function test_equals_to_substring_returns_false() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('ell')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_equals_to_unicode_characters() : void
    {
        self::assertTrue(
            ref('str')->equalsTo('नमस्ते')->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_equals_to_unicode_no_match() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('नमस्कार')->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_equals_to_whitespace_sensitive() : void
    {
        self::assertFalse(
            ref('str')->equalsTo('hello')->eval(
                row(str_entry('str', 'hello '))
            )
        );
    }

    public function test_equals_to_with_scalar_function_parameter() : void
    {
        self::assertTrue(
            ref('str')->equalsTo(ref('compare'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('compare', 'hello')
                )
            )
        );
    }
}

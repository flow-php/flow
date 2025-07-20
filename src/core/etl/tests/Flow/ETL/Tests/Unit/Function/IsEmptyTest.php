<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IsEmptyTest extends FlowTestCase
{
    public function test_is_empty_empty_string() : void
    {
        self::assertTrue(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_is_empty_non_empty_string() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_is_empty_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_is_empty_single_character_string() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_is_empty_string_with_emoji() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', '🚀'))
            )
        );
    }

    public function test_is_empty_string_with_only_newline() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', "\n"))
            )
        );
    }

    public function test_is_empty_string_with_only_space() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', ' '))
            )
        );
    }

    public function test_is_empty_string_with_only_tab() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', "\t"))
            )
        );
    }

    public function test_is_empty_string_with_special_characters() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', '!@#$%'))
            )
        );
    }

    public function test_is_empty_string_with_zero_width_space() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', "\u{200B}"))
            )
        );
    }

    public function test_is_empty_unicode_complex_characters() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_is_empty_unicode_empty_string() : void
    {
        self::assertTrue(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_is_empty_unicode_non_empty_string() : void
    {
        self::assertFalse(
            ref('str')->isEmpty()->eval(
                row(str_entry('str', 'café'))
            )
        );
    }
}

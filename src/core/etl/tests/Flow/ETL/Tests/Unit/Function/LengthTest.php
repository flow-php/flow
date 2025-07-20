<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class LengthTest extends FlowTestCase
{
    public function test_length_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->length()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_length_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->length()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_length_long_string() : void
    {
        $longString = str_repeat('a', 10000);

        self::assertSame(
            10000,
            ref('str')->length()->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_length_mixed_ascii_and_unicode() : void
    {
        self::assertSame(
            9,
            ref('str')->length()->eval(
                row(str_entry('str', 'hello नमस्ते'))
            )
        );
    }

    public function test_length_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->length()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_length_single_character() : void
    {
        self::assertSame(
            1,
            ref('str')->length()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_length_string_with_combining_characters() : void
    {
        self::assertSame(
            1,
            ref('str')->length()->eval(
                row(str_entry('str', 'é'))
            )
        );
    }

    public function test_length_string_with_emoji() : void
    {
        self::assertSame(
            6,
            ref('str')->length()->eval(
                row(str_entry('str', 'world🚀'))
            )
        );
    }

    public function test_length_string_with_whitespace() : void
    {
        self::assertSame(
            12,
            ref('str')->length()->eval(
                row(str_entry('str', 'hello world '))
            )
        );
    }

    public function test_length_unicode_string_with_accented_characters() : void
    {
        self::assertSame(
            4,
            ref('str')->length()->eval(
                row(str_entry('str', 'café'))
            )
        );
    }

    public function test_length_unicode_string_with_complex_characters() : void
    {
        self::assertSame(
            3,
            ref('str')->length()->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_length_unicode_string_with_zero_width_characters() : void
    {
        self::assertSame(
            4,
            ref('str')->length()->eval(
                row(str_entry('str', 'te‌st'))
            )
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class BinaryLengthTest extends FlowTestCase
{
    public function test_binary_length_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_binary_length_binary_data() : void
    {
        $binaryData = "\x00\x01\x02\x03\xFF";

        self::assertSame(
            5,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', $binaryData))
            )
        );
    }

    public function test_binary_length_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_binary_length_long_string() : void
    {
        $longString = str_repeat('a', 10000);

        self::assertSame(
            10000,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_binary_length_mixed_ascii_and_unicode() : void
    {
        self::assertSame(
            24,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'hello नमस्ते'))
            )
        );
    }

    public function test_binary_length_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->binaryLength()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_binary_length_single_ascii_character() : void
    {
        self::assertSame(
            1,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_binary_length_string_with_combining_characters() : void
    {
        self::assertSame(
            2,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'é'))
            )
        );
    }

    public function test_binary_length_string_with_emoji() : void
    {
        self::assertSame(
            9,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'world🚀'))
            )
        );
    }

    public function test_binary_length_string_with_newlines_and_tabs() : void
    {
        self::assertSame(
            12,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', "hello\nworld\t"))
            )
        );
    }

    public function test_binary_length_string_with_zero_width_characters() : void
    {
        self::assertSame(
            7,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'te‌st'))
            )
        );
    }

    public function test_binary_length_unicode_string_with_accented_characters() : void
    {
        self::assertSame(
            5,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'café'))
            )
        );
    }

    public function test_binary_length_unicode_string_with_complex_characters() : void
    {
        self::assertSame(
            18,
            ref('str')->binaryLength()->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_binary_length_vs_character_length_ascii() : void
    {
        $row = row(str_entry('str', 'hello'));

        $binaryLength = ref('str')->binaryLength()->eval($row);
        $characterLength = ref('str')->unicodeLength()->eval($row);

        self::assertSame($binaryLength, $characterLength);
    }

    public function test_binary_length_vs_character_length_emoji() : void
    {
        $row = row(str_entry('str', '🚀'));

        $binaryLength = ref('str')->binaryLength()->eval($row);
        $characterLength = ref('str')->unicodeLength()->eval($row);

        self::assertSame(4, $binaryLength);
        self::assertSame(1, $characterLength);
        self::assertGreaterThan($characterLength, $binaryLength);
    }

    public function test_binary_length_vs_character_length_unicode() : void
    {
        $row = row(str_entry('str', 'café'));

        $binaryLength = ref('str')->binaryLength()->eval($row);
        $characterLength = ref('str')->unicodeLength()->eval($row);

        self::assertSame(5, $binaryLength);
        self::assertSame(4, $characterLength);
        self::assertGreaterThan($characterLength, $binaryLength);
    }
}

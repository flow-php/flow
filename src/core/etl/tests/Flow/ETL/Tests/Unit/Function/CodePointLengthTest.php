<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class CodePointLengthTest extends FlowTestCase
{
    public function test_code_point_length_arabic_with_diacritics() : void
    {
        self::assertSame(
            6,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'مرحباً'))
            )
        );
    }

    public function test_code_point_length_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_code_point_length_combining_characters() : void
    {
        self::assertSame(
            1,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'é'))
            )
        );
    }

    public function test_code_point_length_complex_script_hindi() : void
    {
        self::assertSame(
            6,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_code_point_length_decomposed_combining_characters() : void
    {
        self::assertSame(
            1,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', "e\u{0301}"))
            )
        );
    }

    public function test_code_point_length_emoji_single() : void
    {
        self::assertSame(
            1,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', '🚀'))
            )
        );
    }

    public function test_code_point_length_emoji_with_skin_tone() : void
    {
        self::assertSame(
            2,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', '👋🏻'))
            )
        );
    }

    public function test_code_point_length_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_code_point_length_family_emoji() : void
    {
        self::assertSame(
            7,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', '👨‍👩‍👧‍👦'))
            )
        );
    }

    public function test_code_point_length_long_string() : void
    {
        $longString = str_repeat('a', 10000);

        self::assertSame(
            10000,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_code_point_length_mixed_text_with_emoji() : void
    {
        self::assertSame(
            6,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'world🚀'))
            )
        );
    }

    public function test_code_point_length_multiple_modifiers() : void
    {
        self::assertSame(
            4,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', '🏴‍☠️'))
            )
        );
    }

    public function test_code_point_length_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->codePointLength()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_code_point_length_single_ascii_character() : void
    {
        self::assertSame(
            1,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_code_point_length_surrogate_pairs() : void
    {
        self::assertSame(
            1,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', '𝐇'))
            )
        );
    }

    public function test_code_point_length_thai_with_combining() : void
    {
        self::assertSame(
            6,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', 'สวัสดี'))
            )
        );
    }

    public function test_code_point_length_vs_binary_length_emoji() : void
    {
        $row = row(str_entry('str', '🚀'));

        $codePointLength = ref('str')->codePointLength()->eval($row);
        $binaryLength = ref('str')->binaryLength()->eval($row);

        self::assertSame(1, $codePointLength);
        self::assertSame(4, $binaryLength);
        self::assertLessThan($binaryLength, $codePointLength);
    }

    public function test_code_point_length_vs_character_length_ascii() : void
    {
        $row = row(str_entry('str', 'hello'));

        $codePointLength = ref('str')->codePointLength()->eval($row);
        $characterLength = ref('str')->unicodeLength()->eval($row);

        self::assertSame($codePointLength, $characterLength);
    }

    public function test_code_point_length_vs_other_lengths_comparison() : void
    {
        $complexString = "café🚀e\u{0301}";
        $row = row(str_entry('str', $complexString));

        $codePointLength = ref('str')->codePointLength()->eval($row);
        $unicodeLength = ref('str')->unicodeLength()->eval($row);
        $binaryLength = ref('str')->binaryLength()->eval($row);

        self::assertSame(6, $codePointLength);
        self::assertSame(6, $unicodeLength);
        self::assertSame(12, $binaryLength);

        self::assertSame($unicodeLength, $codePointLength);
        self::assertLessThan($binaryLength, $codePointLength);
    }

    public function test_code_point_length_vs_unicode_length_combining() : void
    {
        $row = row(str_entry('str', "e\u{0301}"));

        $codePointLength = ref('str')->codePointLength()->eval($row);
        $unicodeLength = ref('str')->unicodeLength()->eval($row);

        self::assertSame(1, $codePointLength);
        self::assertSame(1, $unicodeLength);
        self::assertSame($unicodeLength, $codePointLength);
    }

    public function test_code_point_length_vs_unicode_length_emoji_family() : void
    {
        $row = row(str_entry('str', '👨‍👩‍👧‍👦'));

        $codePointLength = ref('str')->codePointLength()->eval($row);
        $unicodeLength = ref('str')->unicodeLength()->eval($row);

        self::assertSame(7, $codePointLength);
        self::assertSame(1, $unicodeLength);
        self::assertGreaterThan($unicodeLength, $codePointLength);
    }

    public function test_code_point_length_zero_width_joiner() : void
    {
        self::assertSame(
            3,
            ref('str')->codePointLength()->eval(
                row(str_entry('str', '👨‍💻'))
            )
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class UnicodeLengthTest extends FlowTestCase
{
    public function test_unicode_length_arabic_with_diacritics() : void
    {
        self::assertSame(
            5,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'مرحباً'))
            )
        );
    }

    public function test_unicode_length_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_unicode_length_combining_characters() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'é'))
            )
        );
    }

    public function test_unicode_length_complex_script_hindi() : void
    {
        self::assertSame(
            3,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_unicode_length_decomposed_combining_characters() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', "e\u{0301}"))
            )
        );
    }

    public function test_unicode_length_emoji_single() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', '🚀'))
            )
        );
    }

    public function test_unicode_length_emoji_with_skin_tone() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', '👋🏻'))
            )
        );
    }

    public function test_unicode_length_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_unicode_length_family_emoji() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', '👨‍👩‍👧‍👦'))
            )
        );
    }

    public function test_unicode_length_long_string() : void
    {
        $longString = str_repeat('a', 10000);

        self::assertSame(
            10000,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_unicode_length_mixed_text_with_emoji() : void
    {
        self::assertSame(
            6,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'world🚀'))
            )
        );
    }

    public function test_unicode_length_multiple_modifiers() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', '🏴‍☠️'))
            )
        );
    }

    public function test_unicode_length_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_unicode_length_single_ascii_character() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_unicode_length_thai_with_combining() : void
    {
        self::assertSame(
            4,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'สวัสดี'))
            )
        );
    }

    public function test_unicode_length_vs_binary_length_combining() : void
    {
        $row = row(str_entry('str', "e\u{0301}"));

        $unicodeLength = ref('str')->unicodeLength()->eval($row);
        $binaryLength = ref('str')->binaryLength()->eval($row);

        self::assertSame(1, $unicodeLength);
        self::assertSame(3, $binaryLength);
        self::assertLessThan($binaryLength, $unicodeLength);
    }

    public function test_unicode_length_vs_binary_length_emoji() : void
    {
        $row = row(str_entry('str', '🚀'));

        $unicodeLength = ref('str')->unicodeLength()->eval($row);
        $binaryLength = ref('str')->binaryLength()->eval($row);

        self::assertSame(1, $unicodeLength);
        self::assertSame(4, $binaryLength);
        self::assertLessThan($binaryLength, $unicodeLength);
    }

    public function test_unicode_length_vs_character_length_ascii() : void
    {
        $row = row(str_entry('str', 'hello'));

        $unicodeLength = ref('str')->unicodeLength()->eval($row);
        $characterLength = ref('str')->length()->eval($row);

        self::assertSame($unicodeLength, $characterLength);
    }

    public function test_unicode_length_vs_character_length_combining() : void
    {
        $row = row(str_entry('str', 'é'));

        $unicodeLength = ref('str')->unicodeLength()->eval($row);
        $characterLength = ref('str')->length()->eval($row);

        self::assertSame(1, $unicodeLength);
        self::assertSame(1, $characterLength);
        self::assertSame($unicodeLength, $characterLength);
    }

    public function test_unicode_length_zero_width_joiner() : void
    {
        self::assertSame(
            1,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', '👨‍💻'))
            )
        );
    }
}

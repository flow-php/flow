<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, row};
use function Flow\ETL\DSL\{ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class StringNormalizeTest extends FlowTestCase
{
    public function test_normalize_already_normalized() : void
    {
        self::assertSame(
            'hello',
            ref('str')->stringNormalize(\Normalizer::NFC)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_normalize_arabic_text() : void
    {
        $arabic = 'مرحبا';
        $normalized = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $arabic))
        );

        self::assertSame($arabic, $normalized);
    }

    public function test_normalize_combining_characters_multiple() : void
    {
        $input = "e\u{0301}\u{0323}";
        $result = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $input))
        );

        self::assertNotNull($result);
    }

    public function test_normalize_comparison_after_normalization() : void
    {
        $composed = 'é';
        $decomposed = "e\u{0301}";

        $normalizedComposed = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $composed))
        );

        $normalizedDecomposed = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $decomposed))
        );

        self::assertSame($normalizedComposed, $normalizedDecomposed);
    }

    public function test_normalize_compatibility_forms() : void
    {
        $superscript = '²';

        $nfkc = ref('str')->stringNormalize(\Normalizer::NFKC)->eval(
            row(str_entry('str', $superscript))
        );

        $nfkd = ref('str')->stringNormalize(\Normalizer::NFKD)->eval(
            row(str_entry('str', $superscript))
        );

        self::assertNotNull($nfkc);
        self::assertNotNull($nfkd);
    }

    public function test_normalize_emoji_with_modifiers() : void
    {
        $emoji = '👋🏻';
        $normalized = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $emoji))
        );

        self::assertSame($emoji, $normalized);
    }

    public function test_normalize_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->stringNormalize()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_normalize_german_umlauts() : void
    {
        $text = 'Müller';
        $normalized = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $text))
        );

        self::assertSame('Müller', $normalized);
    }

    public function test_normalize_greek_text_with_diacritics() : void
    {
        $decomposed = 'καλημέρα';
        $composed = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $decomposed))
        );

        self::assertSame('καλημέρα', $composed);
    }

    public function test_normalize_japanese_text() : void
    {
        $japanese = 'こんにちは';
        $normalized = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $japanese))
        );

        self::assertSame($japanese, $normalized);
    }

    public function test_normalize_nfc_default() : void
    {
        self::assertSame(
            'é',
            ref('str')->stringNormalize()->eval(
                row(str_entry('str', "e\u{0301}"))
            )
        );
    }

    public function test_normalize_nfc_explicit() : void
    {
        self::assertSame(
            'é',
            ref('str')->stringNormalize(\Normalizer::NFC)->eval(
                row(str_entry('str', "e\u{0301}"))
            )
        );
    }

    public function test_normalize_nfd() : void
    {
        self::assertSame(
            "e\u{0301}",
            ref('str')->stringNormalize(\Normalizer::NFD)->eval(
                row(str_entry('str', 'é'))
            )
        );
    }

    public function test_normalize_nfkc() : void
    {
        self::assertSame(
            'ffi',
            ref('str')->stringNormalize(\Normalizer::NFKC)->eval(
                row(str_entry('str', 'ﬃ'))
            )
        );
    }

    public function test_normalize_nfkd() : void
    {
        self::assertSame(
            'ffi',
            ref('str')->stringNormalize(\Normalizer::NFKD)->eval(
                row(str_entry('str', 'ﬃ'))
            )
        );
    }

    public function test_normalize_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->stringNormalize()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_normalize_vietnamese_text() : void
    {
        $decomposed = 'Việt Nam';
        $composed = ref('str')->stringNormalize(\Normalizer::NFC)->eval(
            row(str_entry('str', $decomposed))
        );

        self::assertSame('Việt Nam', $composed);
    }

    public function test_normalize_with_scalar_function_form() : void
    {
        $normalized = ref('str')->stringNormalize(ref('form'))->eval(
            row(str_entry('str', "e\u{0301}"), int_entry('form', \Normalizer::NFC))
        );

        self::assertSame('é', $normalized);
    }
}

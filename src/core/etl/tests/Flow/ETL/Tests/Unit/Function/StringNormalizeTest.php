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

    public function test_normalize_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->stringNormalize()->eval(
                row(str_entry('str', ''))
            )
        );
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

    public function test_normalize_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->stringNormalize()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_normalize_with_scalar_function_form() : void
    {
        $normalized = ref('str')->stringNormalize(ref('form'))->eval(
            row(str_entry('str', "e\u{0301}"), int_entry('form', \Normalizer::NFC))
        );

        self::assertSame('é', $normalized);
    }
}

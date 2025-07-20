<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class WidthTest extends FlowTestCase
{
    public function test_width_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->width()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_width_cjk_characters() : void
    {
        self::assertSame(
            4,
            ref('str')->width()->eval(
                row(str_entry('str', '中文'))
            )
        );
    }

    public function test_width_combining_characters() : void
    {
        self::assertSame(
            1,
            ref('str')->width()->eval(
                row(str_entry('str', 'é'))
            )
        );
    }

    public function test_width_emoji() : void
    {
        self::assertSame(
            2,
            ref('str')->width()->eval(
                row(str_entry('str', '🚀'))
            )
        );
    }

    public function test_width_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->width()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_width_fullwidth_symbols() : void
    {
        self::assertSame(
            2,
            ref('str')->width()->eval(
                row(str_entry('str', '！'))
            )
        );
    }

    public function test_width_japanese_hiragana() : void
    {
        self::assertSame(
            8,
            ref('str')->width()->eval(
                row(str_entry('str', 'ひらがな'))
            )
        );
    }

    public function test_width_korean_hangul() : void
    {
        self::assertSame(
            4,
            ref('str')->width()->eval(
                row(str_entry('str', '한글'))
            )
        );
    }

    public function test_width_mixed_ascii_and_wide() : void
    {
        self::assertSame(
            9,
            ref('str')->width()->eval(
                row(str_entry('str', 'hello中文'))
            )
        );
    }

    public function test_width_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->width()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_width_single_character() : void
    {
        self::assertSame(
            1,
            ref('str')->width()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_width_single_wide_character() : void
    {
        self::assertSame(
            2,
            ref('str')->width()->eval(
                row(str_entry('str', '中'))
            )
        );
    }

    public function test_width_string_with_newline() : void
    {
        self::assertSame(
            0,
            ref('str')->width()->eval(
                row(str_entry('str', "\n"))
            )
        );
    }

    public function test_width_string_with_tab() : void
    {
        self::assertSame(
            0,
            ref('str')->width()->eval(
                row(str_entry('str', "\t"))
            )
        );
    }

    public function test_width_zero_width_space() : void
    {
        self::assertSame(
            0,
            ref('str')->width()->eval(
                row(str_entry('str', "\u{200B}"))
            )
        );
    }
}

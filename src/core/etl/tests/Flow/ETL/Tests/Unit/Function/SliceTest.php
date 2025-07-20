<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class SliceTest extends FlowTestCase
{
    public function test_slice_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->slice(0, 5)->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_slice_entire_string_with_length() : void
    {
        self::assertSame(
            'hello world',
            ref('str')->slice(0, 11)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->slice(1, 3)->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_slice_single_character_string() : void
    {
        self::assertSame(
            'a',
            ref('str')->slice(0)->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_slice_unicode_complex_script() : void
    {
        self::assertSame(
            'मस्ते ',
            ref('str')->slice(1, 3)->eval(
                row(str_entry('str', 'नमस्ते दुनिया'))
            )
        );
    }

    public function test_slice_unicode_string_with_accented_characters() : void
    {
        self::assertSame(
            'afé',
            ref('str')->slice(1, 3)->eval(
                row(str_entry('str', 'café au lait'))
            )
        );
    }

    public function test_slice_unicode_string_with_emoji() : void
    {
        self::assertSame(
            'llo🚀w',
            ref('str')->slice(2, 5)->eval(
                row(str_entry('str', 'hello🚀world'))
            )
        );
    }

    public function test_slice_very_long_string() : void
    {
        $longString = str_repeat('hello world ', 100);

        self::assertSame(
            'hello world hello wo',
            ref('str')->slice(0, 20)->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_slice_whitespace_handling() : void
    {
        self::assertSame(
            ' wor',
            ref('str')->slice(5, 4)->eval(
                row(str_entry('str', 'hello world test'))
            )
        );
    }

    public function test_slice_with_length_zero() : void
    {
        self::assertSame(
            '',
            ref('str')->slice(1, 0)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_negative_length() : void
    {
        self::assertSame(
            'hello wo',
            ref('str')->slice(0, -3)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_negative_start() : void
    {
        self::assertSame(
            'world',
            ref('str')->slice(-5)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_negative_start_and_length() : void
    {
        self::assertSame(
            'wor',
            ref('str')->slice(-5, 3)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_negative_start_beyond_string_length() : void
    {
        self::assertSame(
            'hello world',
            ref('str')->slice(-50)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_null_length() : void
    {
        self::assertSame(
            'ello world',
            ref('str')->slice(1, ref('length'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('length', null)
                )
            )
        );
    }

    public function test_slice_with_null_start() : void
    {
        self::assertSame(
            'hello world',
            ref('str')->slice(ref('start'), 5)->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('start', null)
                )
            )
        );
    }

    public function test_slice_with_positive_start_and_length() : void
    {
        self::assertSame(
            'ell',
            ref('str')->slice(1, 3)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_positive_start_without_length() : void
    {
        self::assertSame(
            'ello world',
            ref('str')->slice(1)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_scalar_function_length() : void
    {
        self::assertSame(
            'ell',
            ref('str')->slice(1, ref('length'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    int_entry('length', 3)
                )
            )
        );
    }

    public function test_slice_with_scalar_function_start() : void
    {
        self::assertSame(
            'ello',
            ref('str')->slice(ref('start'), 4)->eval(
                row(
                    str_entry('str', 'hello world'),
                    int_entry('start', 1)
                )
            )
        );
    }

    public function test_slice_with_scalar_function_start_and_length() : void
    {
        self::assertSame(
            'llo',
            ref('str')->slice(ref('start'), ref('length'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    int_entry('start', 2),
                    int_entry('length', 3)
                )
            )
        );
    }

    public function test_slice_with_start_beyond_string_length() : void
    {
        self::assertSame(
            '',
            ref('str')->slice(20)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_start_zero() : void
    {
        self::assertSame(
            'hello',
            ref('str')->slice(0, 5)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_slice_with_very_large_length() : void
    {
        self::assertSame(
            'ello world',
            ref('str')->slice(1, 1000)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }
}

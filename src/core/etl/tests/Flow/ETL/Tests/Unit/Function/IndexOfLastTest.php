<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{bool_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IndexOfLastTest extends FlowTestCase
{
    public function test_index_of_last_basic() : void
    {
        self::assertSame(
            9,
            ref('str')->indexOfLast('l')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_case_insensitive() : void
    {
        self::assertSame(
            9,
            ref('str')->indexOfLast('L', ignoreCase: true)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_case_sensitive() : void
    {
        self::assertNull(
            ref('str')->indexOfLast('L')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_empty_haystack() : void
    {
        self::assertNull(
            ref('str')->indexOfLast('l')->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_index_of_last_empty_needle() : void
    {
        self::assertNull(
            ref('str')->indexOfLast('')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_identical_strings() : void
    {
        self::assertSame(
            0,
            ref('str')->indexOfLast('hello')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_index_of_last_multiple_occurrences() : void
    {
        self::assertSame(
            7,
            ref('str')->indexOfLast('o')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_needle_longer_than_haystack() : void
    {
        self::assertNull(
            ref('str')->indexOfLast('hello world')->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_index_of_last_not_found() : void
    {
        self::assertNull(
            ref('str')->indexOfLast('x')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_null_needle_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOfLast(ref('needle'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('needle', null)
                )
            )
        );
    }

    public function test_index_of_last_null_string_returns_false() : void
    {
        self::assertFalse(
            ref('str')->indexOfLast('l')->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_index_of_last_overlapping_substrings() : void
    {
        self::assertSame(
            4,
            ref('str')->indexOfLast('aba')->eval(
                row(str_entry('str', 'abababa'))
            )
        );
    }

    public function test_index_of_last_single_character() : void
    {
        self::assertSame(
            0,
            ref('str')->indexOfLast('a')->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_index_of_last_unicode_characters() : void
    {
        self::assertSame(
            2,
            ref('str')->indexOfLast('स्ते')->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_index_of_last_with_offset() : void
    {
        self::assertSame(
            9,
            ref('str')->indexOfLast('l', offset: 5)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_index_of_last_with_scalar_function_parameters() : void
    {
        self::assertSame(
            9,
            ref('str')->indexOfLast(ref('needle'), ref('ignore_case'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('needle', 'L'),
                    bool_entry('ignore_case', true)
                )
            )
        );
    }
}

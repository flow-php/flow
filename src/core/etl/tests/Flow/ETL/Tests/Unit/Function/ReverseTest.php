<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class ReverseTest extends FlowTestCase
{
    public function test_reverse_ascii_string() : void
    {
        self::assertSame(
            'olleh',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_reverse_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->reverse()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_reverse_long_string() : void
    {
        $longString = str_repeat('abcdef', 1000);
        $expectedReverse = str_repeat('fedcba', 1000);

        self::assertSame(
            $expectedReverse,
            ref('str')->reverse()->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_reverse_palindrome() : void
    {
        self::assertSame(
            'racecar',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'racecar'))
            )
        );
    }

    public function test_reverse_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->reverse()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_reverse_single_character() : void
    {
        self::assertSame(
            'a',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_reverse_string_with_whitespace() : void
    {
        self::assertSame(
            ' dlrow olleh',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'hello world '))
            )
        );
    }

    public function test_reverse_unicode_string_with_accented_characters() : void
    {
        self::assertSame(
            'éfac',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'café'))
            )
        );
    }

    public function test_reverse_unicode_string_with_complex_characters() : void
    {
        self::assertSame(
            'तेस्मन',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_reverse_unicode_string_with_emoji() : void
    {
        self::assertSame(
            '🚀dlrow',
            ref('str')->reverse()->eval(
                row(str_entry('str', 'world🚀'))
            )
        );
    }
}

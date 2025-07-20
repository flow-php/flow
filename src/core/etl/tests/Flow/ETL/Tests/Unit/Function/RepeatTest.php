<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class RepeatTest extends FlowTestCase
{
    public function test_repeat_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->repeat(3)->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_repeat_negative_times() : void
    {
        self::assertSame(
            '',
            ref('str')->repeat(-1)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_repeat_null_input() : void
    {
        self::assertNull(
            ref('str')->repeat(3)->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_repeat_once() : void
    {
        self::assertSame(
            'hello',
            ref('str')->repeat(1)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_repeat_single_character() : void
    {
        self::assertSame(
            'aaaaa',
            ref('str')->repeat(5)->eval(
                row(str_entry('str', 'a'))
            )
        );
    }

    public function test_repeat_string_multiple_times() : void
    {
        self::assertSame(
            'hellohellohello',
            ref('str')->repeat(3)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_repeat_unicode_complex_script() : void
    {
        self::assertSame(
            'नमस्तेनमस्तेनमस्ते',
            ref('str')->repeat(3)->eval(
                row(str_entry('str', 'नमस्ते'))
            )
        );
    }

    public function test_repeat_unicode_string_with_accented_characters() : void
    {
        self::assertSame(
            'cafécafécafé',
            ref('str')->repeat(3)->eval(
                row(str_entry('str', 'café'))
            )
        );
    }

    public function test_repeat_unicode_string_with_emoji() : void
    {
        self::assertSame(
            '🚀🚀🚀',
            ref('str')->repeat(3)->eval(
                row(str_entry('str', '🚀'))
            )
        );
    }

    public function test_repeat_with_large_repetition_count() : void
    {
        $result = ref('str')->repeat(1000)->eval(
            row(str_entry('str', 'a'))
        );

        self::assertSame(1000, strlen((string) $result));
        self::assertSame(str_repeat('a', 1000), $result);
    }

    public function test_repeat_with_null_times() : void
    {
        self::assertSame(
            '',
            ref('str')->repeat(ref('times'))->eval(
                row(
                    str_entry('str', 'hello'),
                    str_entry('times', null)
                )
            )
        );
    }

    public function test_repeat_with_scalar_function_times() : void
    {
        self::assertSame(
            'hellohello',
            ref('str')->repeat(ref('times'))->eval(
                row(
                    str_entry('str', 'hello'),
                    int_entry('times', 2)
                )
            )
        );
    }

    public function test_repeat_with_whitespace() : void
    {
        self::assertSame(
            '   ',
            ref('str')->repeat(3)->eval(
                row(str_entry('str', ' '))
            )
        );
    }

    public function test_repeat_zero_times() : void
    {
        self::assertSame(
            '',
            ref('str')->repeat(0)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }
}

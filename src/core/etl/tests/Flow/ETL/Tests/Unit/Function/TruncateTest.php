<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{int_entry, ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class TruncateTest extends FlowTestCase
{
    public function test_truncate_ellipsis_longer_than_limit() : void
    {
        self::assertSame(
            'hello',
            ref('str')->truncate(5, 'verylongellipsis')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_truncate_empty_string() : void
    {
        self::assertSame(
            '',
            ref('str')->truncate(10)->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_truncate_exact_length() : void
    {
        self::assertSame(
            'hello',
            ref('str')->truncate(5)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_truncate_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->truncate(10)->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_truncate_string_longer_than_limit() : void
    {
        self::assertSame(
            'he...',
            ref('str')->truncate(5)->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_truncate_string_shorter_than_limit() : void
    {
        self::assertSame(
            'hello',
            ref('str')->truncate(10)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_truncate_unicode_complex_script() : void
    {
        self::assertSame(
            'न...',
            ref('str')->truncate(4)->eval(
                row(str_entry('str', 'नमस्ते दुनिया'))
            )
        );
    }

    public function test_truncate_unicode_string_with_accented_characters() : void
    {
        self::assertSame(
            'c...',
            ref('str')->truncate(4)->eval(
                row(str_entry('str', 'café au lait'))
            )
        );
    }

    public function test_truncate_unicode_string_with_emoji() : void
    {
        self::assertSame(
            'he...',
            ref('str')->truncate(5)->eval(
                row(str_entry('str', 'hello🚀world'))
            )
        );
    }

    public function test_truncate_very_long_string() : void
    {
        $longString = str_repeat('hello world ', 100);

        self::assertSame(
            'hello world hello...',
            ref('str')->truncate(20)->eval(
                row(str_entry('str', $longString))
            )
        );
    }

    public function test_truncate_with_custom_ellipsis() : void
    {
        self::assertSame(
            'he***',
            ref('str')->truncate(5, '***')->eval(
                row(str_entry('str', 'hello world'))
            )
        );
    }

    public function test_truncate_with_length_one() : void
    {
        self::assertSame(
            'h',
            ref('str')->truncate(1)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_truncate_with_length_zero() : void
    {
        self::assertSame(
            '',
            ref('str')->truncate(0)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_truncate_with_negative_length() : void
    {
        self::assertSame(
            'hell',
            ref('str')->truncate(-1)->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_truncate_with_null_length() : void
    {
        self::assertSame(
            'hello world',
            ref('str')->truncate(ref('length'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('length', null)
                )
            )
        );
    }

    public function test_truncate_with_scalar_function_ellipsis() : void
    {
        self::assertSame(
            'hel>>',
            ref('str')->truncate(5, ref('ellipsis'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    str_entry('ellipsis', '>>')
                )
            )
        );
    }

    public function test_truncate_with_scalar_function_length() : void
    {
        self::assertSame(
            'he...',
            ref('str')->truncate(ref('length'))->eval(
                row(
                    str_entry('str', 'hello world'),
                    int_entry('length', 5)
                )
            )
        );
    }

    public function test_truncate_with_whitespace() : void
    {
        self::assertSame(
            'hell...',
            ref('str')->truncate(7)->eval(
                row(str_entry('str', 'hello world test'))
            )
        );
    }
}

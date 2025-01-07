<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{int_entry, lit, ref, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class SanitizeTest extends FlowTestCase
{
    public function test_sanitize_on_non_string_value() : void
    {
        self::assertNull(
            ref('value')->sanitize()->eval(row(int_entry('value', 1000))),
        );
    }

    public function test_sanitize_on_valid_string() : void
    {
        self::assertSame(
            '****',
            ref('value')->sanitize()->eval(row(str_entry('value', 'test'))),
        );
    }

    public function test_sanitize_on_valid_string_with_left_characters() : void
    {
        self::assertSame(
            'te**',
            ref('value')->sanitize(skipCharacters: lit(2))->eval(row(str_entry('value', 'test'))),
        );
    }

    public function test_sanitize_on_valid_string_with_left_characters_longer_than_string() : void
    {
        self::assertSame(
            '****',
            ref('value')->sanitize(skipCharacters: lit(5))->eval(row(str_entry('value', 'test'))),
        );
    }

    public function test_sanitize_on_valid_string_with_placeholder() : void
    {
        self::assertSame(
            '----',
            ref('value')->sanitize(lit('-'))->eval(row(str_entry('value', 'test'))),
        );
    }
}

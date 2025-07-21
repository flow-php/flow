<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class UnicodeLengthTest extends FlowTestCase
{
    public function test_unicode_length_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', 'hello'))
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

    public function test_unicode_length_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->unicodeLength()->eval(
                row(str_entry('str', null))
            )
        );
    }
}

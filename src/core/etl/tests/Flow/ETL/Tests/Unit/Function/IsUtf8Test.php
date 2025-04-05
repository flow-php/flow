<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class IsUtf8Test extends FlowTestCase
{
    public function test_is_utf8_returns_null() : void
    {
        self::assertFalse(
            ref('str')->isUtf8()->eval(
                row(
                    str_entry('str', null),
                )
            )
        );
    }

    public function test_is_utf_8() : void
    {
        self::assertTrue(
            ref('str')->isUtf8()->eval(
                row(str_entry('str', 'Lorem Ipsum'))
            )
        );

        self::assertFalse(
            ref('str')->isUtf8()->eval(
                row(str_entry('str', "\xc3\x28"))
            )
        );
    }
}

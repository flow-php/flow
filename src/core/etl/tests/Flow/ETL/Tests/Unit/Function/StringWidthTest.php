<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, str_entry};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class StringWidthTest extends FlowTestCase
{
    public function test_width_ascii_string() : void
    {
        self::assertSame(
            5,
            ref('str')->stringWidth()->eval(
                row(str_entry('str', 'hello'))
            )
        );
    }

    public function test_width_empty_string() : void
    {
        self::assertSame(
            0,
            ref('str')->stringWidth()->eval(
                row(str_entry('str', ''))
            )
        );
    }

    public function test_width_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->stringWidth()->eval(
                row(str_entry('str', null))
            )
        );
    }

    public function test_width_single_character() : void
    {
        self::assertSame(
            1,
            ref('str')->stringWidth()->eval(
                row(str_entry('str', 'a'))
            )
        );
    }
}

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

    public function test_reverse_returns_null_for_null_input() : void
    {
        self::assertNull(
            ref('str')->reverse()->eval(
                row(str_entry('str', null))
            )
        );
    }
}

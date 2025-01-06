<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{ref, row, str_entry};
use Flow\ETL\Tests\FlowTestCase;

final class AsciiTest extends FlowTestCase
{
    public function test_ascii() : void
    {
        self::assertSame(
            'azcz',
            ref('str')->ascii()->eval(row(str_entry('str', 'ąźćż')))
        );
    }

    public function test_ascii_on_null() : void
    {
        self::assertNull(
            ref('str')->ascii()->eval(row(str_entry('str', null)))
        );
    }
}

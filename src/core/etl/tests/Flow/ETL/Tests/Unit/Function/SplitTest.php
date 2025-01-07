<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, split};
use Flow\ETL\Tests\FlowTestCase;

final class SplitTest extends FlowTestCase
{
    public function test_split_not_string() : void
    {
        self::assertNull(
            split(lit(123), ',')->eval(row())
        );
    }

    public function test_split_string() : void
    {
        self::assertSame(
            ['foo', 'bar', 'baz'],
            split(lit('foo,bar,baz'), ',')->eval(row())
        );
    }

    public function test_split_string_with_limit() : void
    {
        self::assertSame(
            ['foo', 'bar,baz'],
            split(lit('foo,bar,baz'), ',', 2)->eval(row())
        );
    }
}

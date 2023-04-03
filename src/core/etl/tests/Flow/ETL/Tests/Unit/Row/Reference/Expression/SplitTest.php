<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\split;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class SplitTest extends TestCase
{
    public function test_split_not_string() : void
    {
        $this->assertSame(
            123,
            split(lit(123), ',')->eval(Row::create())
        );
    }

    public function test_split_string() : void
    {
        $this->assertSame(
            ['foo', 'bar', 'baz'],
            split(lit('foo,bar,baz'), ',')->eval(Row::create())
        );
    }

    public function test_split_string_with_limit() : void
    {
        $this->assertSame(
            ['foo', 'bar,baz'],
            split(lit('foo,bar,baz'), ',', 2)->eval(Row::create())
        );
    }
}

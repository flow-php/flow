<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Row\Reference\Expression;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\size;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class SizeTest extends TestCase
{
    public function test_size_expression_on_array_value() : void
    {
        $this->assertSame(
            3,
            size(lit(['foo', 'bar', 'baz']))->eval(Row::create())
        );
    }

    public function test_size_expression_on_integer_value() : void
    {
        $this->assertNull(
            size(lit(1))->eval(Row::create())
        );
    }

    public function test_size_expression_on_string_value() : void
    {
        $this->assertSame(
            3,
            size(lit('foo'))->eval(Row::create())
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\sprintf;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class SprintfTest extends TestCase
{
    public function test_sprintf_expression_on_invalid_format() : void
    {
        $sprintf = sprintf(
            lit(1),
            lit('John'),
            lit(25)
        );

        $this->assertNull($sprintf->eval(Row::create()));
    }

    public function test_sprintf_expression_on_valid_format_and_args() : void
    {
        $sprintf = sprintf(
            lit('Hello, %s! Your age is %d.'),
            lit('John'),
            lit(25)
        );

        $this->assertSame(
            'Hello, John! Your age is 25.',
            $sprintf->eval(Row::create())
        );
    }
}

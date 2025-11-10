<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\{flow_context, lit, sprintf};
use function Flow\ETL\DSL\row;
use Flow\ETL\Tests\FlowTestCase;

final class SprintfTest extends FlowTestCase
{
    public function test_sprintf_expression_on_invalid_format() : void
    {
        $sprintf = sprintf(
            lit(1),
            lit('John'),
            lit(25)
        );

        self::assertNull($sprintf->eval(row(), flow_context()));
    }

    public function test_sprintf_expression_on_valid_format_and_args() : void
    {
        $sprintf = sprintf(
            lit('Hello, %s! Your age is %d.'),
            lit('John'),
            lit(25)
        );

        self::assertSame(
            'Hello, John! Your age is 25.',
            $sprintf->eval(row(), flow_context())
        );
    }
}

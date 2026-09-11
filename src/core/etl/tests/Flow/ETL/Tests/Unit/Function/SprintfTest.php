<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\sprintf;

final class SprintfTest extends FlowTestCase
{
    public function test_sprintf_expression_on_invalid_format(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        $sprintf = sprintf(lit(1), lit('John'), lit(25));

        $sprintf->eval(row([]), flow_context());
    }

    public function test_sprintf_expression_on_valid_format_and_args(): void
    {
        $sprintf = sprintf(lit('Hello, %s! Your age is %d.'), lit('John'), lit(25));

        static::assertSame('Hello, John! Your age is 25.', $sprintf->eval(row([]), flow_context()));
    }
}

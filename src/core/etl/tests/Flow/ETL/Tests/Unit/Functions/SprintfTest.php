<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Sprintf;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\sprintf;

final class SprintfTest extends FlowTestCase
{
    public function test_sprintf_expression_on_invalid_format(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(sprintf(lit(1), lit('John'), lit(25)), [], schema());
    }

    public function test_sprintf_expression_on_valid_format_and_args(): void
    {
        static::assertSame('Hello, John! Your age is 25.', (new FunctionContext(flow_context()))->eval(
            sprintf(lit('Hello, %s! Your age is %d.'), lit('John'), lit(25)),
            [],
            schema(),
        ));
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new Sprintf(lit('%s-%d'), ref('string'), ref('integer'));
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}

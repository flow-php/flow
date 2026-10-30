<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\RegexAll;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\regex_all;
use function Flow\ETL\DSL\schema;

final class RegexAllTest extends FlowTestCase
{
    public function test_regex_all_expression_on_invalid_subject(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex_all(lit('/\d+/'), lit(2)), [], schema());
    }

    public function test_regex_all_expression_on_no_match(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            regex_all(lit('/\d+/'), lit('apples and oranges')),
            [],
            schema(),
        ));
    }

    public function test_regex_all_expression_on_valid_strings(): void
    {
        static::assertEquals(
            [
                ['124.23 EUR', '12 USD', '45 PLN'],
                ['124.23',     '12',     '45'],
                ['EUR',        'USD',    'PLN'],
            ],
            (new FunctionContext(flow_context()))->eval(
                regex_all(lit('/(\d+(?:\.\d+)?)\s+([A-Z]{3})/'), lit('124.23 EUR 12 USD 45 PLN')),
                [],
                schema(),
            ),
        );
    }

    public function test_regex_expression_on_invalid_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex_all(lit(1), lit('12 apples and 45 oranges')), [], schema());
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new RegexAll(lit('/a/'), ref('string'), 0, 1);
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }
}

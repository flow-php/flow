<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Functions\Regex;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Tests\Context\DescribeRoundTrip;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\Context\ScalarFunctionFixtures;
use Flow\ETL\Tests\FlowTestCase;
use Flow\ETL\Values\Column\PhpBackend;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\ref;
use function Flow\ETL\DSL\regex;
use function Flow\ETL\DSL\regex_match_all;
use function Flow\ETL\DSL\schema;

use const PREG_OFFSET_CAPTURE;

final class RegexTest extends FlowTestCase
{
    public function test_regex_expression_on_invalid_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex(lit(1), lit('12 apples and 45 oranges')), [], schema());
    }

    public function test_regex_expression_on_invalid_subject(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex(lit('/\d+/'), lit(2)), [], schema());
    }

    public function test_regex_expression_on_no_match(): void
    {
        static::assertNull((new FunctionContext(flow_context()))->eval(
            regex(lit('/\d+/'), lit('apples and oranges')),
            [],
            schema(),
        ));
    }

    public function test_regex_expression_on_valid_strings(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            regex_match_all(lit('/\d+/'), lit('12 apples and 45 oranges')),
            [],
            schema(),
        ));
    }

    public function test_describe_round_trips(): void
    {
        $context = new FunctionContext();
        $function = new Regex(lit('/a/'), ref('string'), 0, 1);
        $rebuilt = DescribeRoundTrip::rebuilt($function);
        $rows = ScalarFunctionFixtures::rows(new PhpBackend());
        $schema = ScalarFunctionFixtures::schema();

        static::assertSame($context->exported($function, $rows, $schema), $context->exported($rebuilt, $rows, $schema));
    }

    public function test_a_description_rebuilds_through_the_constructor_checks(): void
    {
        $data = [
            'class' => Regex::class,
            'secret' => false,
            'data' => [
                'pattern' => lit('/a/')->describe()?->normalize(),
                'subject' => ref('string')->describe()?->normalize(),
                'flags' => PREG_OFFSET_CAPTURE,
                'offset' => lit(0)->describe()?->normalize(),
            ],
        ];

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Regex does not support PREG_OFFSET_CAPTURE');

        (new Rebuild())->of($data, ScalarFunction::class);
    }
}

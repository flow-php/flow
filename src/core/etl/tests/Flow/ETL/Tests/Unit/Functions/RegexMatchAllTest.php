<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_match_all;
use function Flow\ETL\DSL\schema;

final class RegexMatchAllTest extends FlowTestCase
{
    public function test_regex_match_all_expression_on_invalid_flags(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "integer", got "string".');

        (new FunctionContext(flow_context()))->eval(
            regex_match_all(lit('/\d+/'), lit('12 apples and 45 oranges'), lit('invalid')),
            [],
            schema(),
        );
    }

    public function test_regex_match_all_expression_on_invalid_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(
            regex_match_all(lit(1), lit('12 apples and 45 oranges')),
            [],
            schema(),
        );
    }

    public function test_regex_match_all_expression_on_invalid_subject(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex_match_all(lit('/\d+/'), lit(2)), [], schema());
    }

    public function test_regex_match_all_expression_on_valid_strings(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            regex_match_all(lit('/\d+/'), lit('12 apples and 45 oranges')),
            [],
            schema(),
        ));
    }

    public function test_regex_match_all_expression_on_valid_strings_with_flags(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            regex_match_all(
                lit('/(\d+(?:\.\d+)?)\s+([A-Z]{3})/'),
                lit('124.23 EUR 12 USD 45 PLN'),
                lit(PREG_PATTERN_ORDER),
            ),
            [],
            schema(),
        ));
    }
}

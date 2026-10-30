<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Functions;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Context\FunctionContext;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_match;
use function Flow\ETL\DSL\schema;

final class RegexMatchTest extends FlowTestCase
{
    public function test_regex_match_expression_on_invalid_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex_match(lit(1), lit('12 apples and 45 oranges')), [], schema());
    }

    public function test_regex_match_expression_on_invalid_subject(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        (new FunctionContext(flow_context()))->eval(regex_match(lit('/\d+/'), lit(2)), [], schema());
    }

    public function test_regex_match_expression_on_no_match(): void
    {
        static::assertFalse((new FunctionContext(flow_context()))->eval(
            regex_match(lit('/\d+/'), lit('apples and oranges')),
            [],
            schema(),
        ));
    }

    public function test_regex_match_expression_on_valid_strings(): void
    {
        static::assertTrue((new FunctionContext(flow_context()))->eval(
            regex_match(lit('/\d+/'), lit('12 apples and 45 oranges')),
            [],
            schema(),
        ));
    }
}

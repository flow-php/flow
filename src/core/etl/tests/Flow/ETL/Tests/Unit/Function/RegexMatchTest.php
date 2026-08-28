<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_match;
use function Flow\ETL\DSL\row;

final class RegexMatchTest extends FlowTestCase
{
    public function test_regex_match_expression_on_invalid_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        $pregMatch = regex_match(lit(1), lit('12 apples and 45 oranges'));

        $pregMatch->eval(row(), flow_context());
    }

    public function test_regex_match_expression_on_invalid_subject(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected type "string", got "integer".');

        $pregMatch = regex_match(lit('/\d+/'), lit(2));

        $pregMatch->eval(row(), flow_context());
    }

    public function test_regex_match_expression_on_no_match(): void
    {
        $pregMatch = regex_match(lit('/\d+/'), lit('apples and oranges'));

        static::assertFalse($pregMatch->eval(row(), flow_context()));
    }

    public function test_regex_match_expression_on_valid_strings(): void
    {
        $pregMatch = regex_match(lit('/\d+/'), lit('12 apples and 45 oranges'));

        static::assertTrue($pregMatch->eval(row(), flow_context()));
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex;
use function Flow\ETL\DSL\regex_match_all;
use function Flow\ETL\DSL\row;

final class RegexTest extends FlowTestCase
{
    public function test_regex_expression_on_invalid_pattern(): void
    {
        $pregMatch = regex(lit(1), lit('12 apples and 45 oranges'));

        static::assertNull($pregMatch->eval(row(), flow_context()));
    }

    public function test_regex_expression_on_invalid_subject(): void
    {
        $pregMatch = regex(lit('/\d+/'), lit(2));

        static::assertNull($pregMatch->eval(row(), flow_context()));
    }

    public function test_regex_expression_on_no_match(): void
    {
        $pregMatch = regex(lit('/\d+/'), lit('apples and oranges'));

        static::assertNull($pregMatch->eval(row(), flow_context()));
    }

    public function test_regex_expression_on_valid_strings(): void
    {
        $pregMatch = regex_match_all(lit('/\d+/'), lit('12 apples and 45 oranges'));

        static::assertTrue($pregMatch->eval(row(), flow_context()));
    }
}

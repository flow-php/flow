<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_match_all;
use function Flow\ETL\DSL\row;

final class RegexMatchAllTest extends FlowTestCase
{
    public function test_regex_match_all_expression_on_invalid_flags(): void
    {
        $pregMatchAll = regex_match_all(lit('/\d+/'), lit('12 apples and 45 oranges'), lit('invalid'));

        static::assertNull($pregMatchAll->eval(row(), flow_context()));
    }

    public function test_regex_match_all_expression_on_invalid_pattern(): void
    {
        $pregMatchAll = regex_match_all(lit(1), lit('12 apples and 45 oranges'));

        static::assertNull($pregMatchAll->eval(row(), flow_context()));
    }

    public function test_regex_match_all_expression_on_invalid_subject(): void
    {
        $pregMatchAll = regex_match_all(lit('/\d+/'), lit(2));

        static::assertNull($pregMatchAll->eval(row(), flow_context()));
    }

    public function test_regex_match_all_expression_on_valid_strings(): void
    {
        $pregMatchAll = regex_match_all(lit('/\d+/'), lit('12 apples and 45 oranges'));

        static::assertTrue($pregMatchAll->eval(row(), flow_context()));
    }

    public function test_regex_match_all_expression_on_valid_strings_with_flags(): void
    {
        $pregMatchAll = regex_match_all(
            lit('/(\d+(?:\.\d+)?)\s+([A-Z]{3})/'),
            lit('124.23 EUR 12 USD 45 PLN'),
            lit(PREG_PATTERN_ORDER),
        );

        static::assertTrue($pregMatchAll->eval(row(), flow_context()));
    }
}

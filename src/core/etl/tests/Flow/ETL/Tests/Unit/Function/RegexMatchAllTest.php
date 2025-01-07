<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, regex_match_all};
use Flow\ETL\Tests\FlowTestCase;

final class RegexMatchAllTest extends FlowTestCase
{
    public function test_regex_match_all_expression_on_invalid_flags() : void
    {
        $pregMatchAll = regex_match_all(
            lit('/\d+/'),
            lit('12 apples and 45 oranges'),
            lit('invalid')
        );

        self::assertNull(
            $pregMatchAll->eval(row())
        );
    }

    public function test_regex_match_all_expression_on_invalid_pattern() : void
    {
        $pregMatchAll = regex_match_all(
            lit(1),
            lit('12 apples and 45 oranges')
        );

        self::assertNull(
            $pregMatchAll->eval(row())
        );
    }

    public function test_regex_match_all_expression_on_invalid_subject() : void
    {
        $pregMatchAll = regex_match_all(
            lit('/\d+/'),
            lit(2)
        );

        self::assertNull(
            $pregMatchAll->eval(row())
        );
    }

    public function test_regex_match_all_expression_on_valid_strings() : void
    {
        $pregMatchAll = regex_match_all(
            lit('/\d+/'),
            lit('12 apples and 45 oranges')
        );

        self::assertTrue(
            $pregMatchAll->eval(row())
        );
    }

    public function test_regex_match_all_expression_on_valid_strings_with_flags() : void
    {
        $pregMatchAll = regex_match_all(
            lit('/(\d+(?:\.\d+)?)\s+([A-Z]{3})/'),
            lit('124.23 EUR 12 USD 45 PLN'),
            lit(PREG_PATTERN_ORDER)
        );

        self::assertTrue(
            $pregMatchAll->eval(row())
        );
    }
}

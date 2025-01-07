<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, regex_match};
use Flow\ETL\Tests\FlowTestCase;

final class RegexMatchTest extends FlowTestCase
{
    public function test_regex_match_expression_on_invalid_pattern() : void
    {
        $pregMatch = regex_match(
            lit(1),
            lit('12 apples and 45 oranges')
        );

        self::assertNull($pregMatch->eval(row()));
    }

    public function test_regex_match_expression_on_invalid_subject() : void
    {
        $pregMatch = regex_match(
            lit('/\d+/'),
            lit(2)
        );

        self::assertNull($pregMatch->eval(row()));
    }

    public function test_regex_match_expression_on_no_match() : void
    {
        $pregMatch = regex_match(
            lit('/\d+/'),
            lit('apples and oranges')
        );

        self::assertFalse($pregMatch->eval(row()));
    }

    public function test_regex_match_expression_on_valid_strings() : void
    {
        $pregMatch = regex_match(
            lit('/\d+/'),
            lit('12 apples and 45 oranges')
        );

        self::assertTrue($pregMatch->eval(row()));
    }
}

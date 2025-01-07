<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, regex_all};
use Flow\ETL\Tests\FlowTestCase;

final class RegexAllTest extends FlowTestCase
{
    public function test_regex_all_expression_on_invalid_subject() : void
    {
        $pregMatch = regex_all(
            lit('/\d+/'),
            lit(2)
        );

        self::assertNull($pregMatch->eval(row()));
    }

    public function test_regex_all_expression_on_no_match() : void
    {
        $pregMatch = regex_all(
            lit('/\d+/'),
            lit('apples and oranges')
        );

        self::assertNull($pregMatch->eval(row()));
    }

    public function test_regex_all_expression_on_valid_strings() : void
    {
        $pregMatch = regex_all(
            lit('/(\d+(?:\.\d+)?)\s+([A-Z]{3})/'),
            lit('124.23 EUR 12 USD 45 PLN')
        );

        self::assertEquals(
            [['124.23 EUR', '12 USD', '45 PLN'], ['124.23', '12', '45'], ['EUR', 'USD', 'PLN']],
            $pregMatch->eval(row())
        );
    }

    public function test_regex_expression_on_invalid_pattern() : void
    {
        $pregMatch = regex_all(
            lit(1),
            lit('12 apples and 45 oranges')
        );

        self::assertNull($pregMatch->eval(row()));
    }
}

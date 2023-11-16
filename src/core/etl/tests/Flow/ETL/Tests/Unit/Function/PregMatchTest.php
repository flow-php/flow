<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_match;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class PregMatchTest extends TestCase
{
    public function test_preg_match_expression_on_invalid_pattern() : void
    {
        $pregMatch = regex_match(
            lit(1),
            lit('12 apples and 45 oranges')
        );

        $this->assertNull($pregMatch->eval(Row::create()));
    }

    public function test_preg_match_expression_on_invalid_subject() : void
    {
        $pregMatch = regex_match(
            lit('/\d+/'),
            lit(2)
        );

        $this->assertNull($pregMatch->eval(Row::create()));
    }

    public function test_preg_match_expression_on_no_match() : void
    {
        $pregMatch = regex_match(
            lit('/\d+/'),
            lit('apples and oranges')
        );

        $this->assertFalse($pregMatch->eval(Row::create()));
    }

    public function test_preg_match_expression_on_valid_strings() : void
    {
        $pregMatch = regex_match(
            lit('/\d+/'),
            lit('12 apples and 45 oranges')
        );

        $this->assertTrue($pregMatch->eval(Row::create()));
    }
}

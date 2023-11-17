<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_replace;
use Flow\ETL\Row;
use PHPUnit\Framework\TestCase;

final class PregReplaceTest extends TestCase
{
    public function test_preg_replace_expression_on_invalid_pattern() : void
    {
        $pregReplace = regex_replace(
            lit(1),
            lit('bar'),
            lit('foo is awesome')
        );

        $this->assertNull($pregReplace->eval(Row::create()));
    }

    public function test_preg_replace_expression_on_invalid_replacement() : void
    {
        $pregReplace = regex_replace(
            lit('/(foo)/'),
            lit(2),
            lit('foo is awesome')
        );

        $this->assertNull($pregReplace->eval(Row::create()));
    }

    public function test_preg_replace_expression_on_invalid_subject() : void
    {
        $pregReplace = regex_replace(
            lit('/(foo)/'),
            lit('bar'),
            lit(3)
        );

        $this->assertNull($pregReplace->eval(Row::create()));
    }

    public function test_preg_replace_expression_on_valid_strings() : void
    {
        $pregReplace = regex_replace(
            lit('/(foo)/'),
            lit('bar'),
            lit('foo is awesome')
        );

        $this->assertSame(
            'bar is awesome',
            $pregReplace->eval(Row::create())
        );
    }
}

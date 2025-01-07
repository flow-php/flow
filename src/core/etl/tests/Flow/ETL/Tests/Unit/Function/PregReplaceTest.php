<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\{lit, regex_replace};
use Flow\ETL\Tests\FlowTestCase;

final class PregReplaceTest extends FlowTestCase
{
    public function test_preg_replace_expression_on_invalid_pattern() : void
    {
        $pregReplace = regex_replace(
            lit(1),
            lit('bar'),
            lit('foo is awesome')
        );

        self::assertNull($pregReplace->eval(row()));
    }

    public function test_preg_replace_expression_on_invalid_replacement() : void
    {
        $pregReplace = regex_replace(
            lit('/(foo)/'),
            lit(2),
            lit('foo is awesome')
        );

        self::assertNull($pregReplace->eval(row()));
    }

    public function test_preg_replace_expression_on_invalid_subject() : void
    {
        $pregReplace = regex_replace(
            lit('/(foo)/'),
            lit('bar'),
            lit(3)
        );

        self::assertNull($pregReplace->eval(row()));
    }

    public function test_preg_replace_expression_on_valid_strings() : void
    {
        $pregReplace = regex_replace(
            lit('/(foo)/'),
            lit('bar'),
            lit('foo is awesome')
        );

        self::assertSame(
            'bar is awesome',
            $pregReplace->eval(row())
        );
    }
}

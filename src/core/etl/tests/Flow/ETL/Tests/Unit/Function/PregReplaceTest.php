<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_replace;
use function Flow\ETL\DSL\row;

final class PregReplaceTest extends FlowTestCase
{
    public function test_preg_replace_expression_on_invalid_pattern(): void
    {
        $pregReplace = regex_replace(lit(1), lit('bar'), lit('foo is awesome'));

        static::assertNull($pregReplace->eval(row(), flow_context()));
    }

    public function test_preg_replace_expression_on_invalid_replacement(): void
    {
        $pregReplace = regex_replace(lit('/(foo)/'), lit(2), lit('foo is awesome'));

        static::assertNull($pregReplace->eval(row(), flow_context()));
    }

    public function test_preg_replace_expression_on_invalid_subject(): void
    {
        $pregReplace = regex_replace(lit('/(foo)/'), lit('bar'), lit(3));

        static::assertNull($pregReplace->eval(row(), flow_context()));
    }

    public function test_preg_replace_expression_on_valid_strings(): void
    {
        $pregReplace = regex_replace(lit('/(foo)/'), lit('bar'), lit('foo is awesome'));

        static::assertSame('bar is awesome', $pregReplace->eval(row(), flow_context()));
    }
}

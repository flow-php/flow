<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\lit;
use function Flow\ETL\DSL\regex_replace;
use function Flow\ETL\DSL\row;

final class PregReplaceTest extends FlowTestCase
{
    public function test_preg_replace_expression_on_invalid_pattern(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('RegexReplace requires non-null pattern');

        $pregReplace = regex_replace(lit(1), lit('bar'), lit('foo is awesome'));

        $pregReplace->eval(row(), flow_context());
    }

    public function test_preg_replace_expression_on_invalid_replacement(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('RegexReplace requires non-null replacement');

        $pregReplace = regex_replace(lit('/(foo)/'), lit(2), lit('foo is awesome'));

        $pregReplace->eval(row(), flow_context());
    }

    public function test_preg_replace_expression_on_invalid_subject(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('RegexReplace requires non-null subject');

        $pregReplace = regex_replace(lit('/(foo)/'), lit('bar'), lit(3));

        $pregReplace->eval(row(), flow_context());
    }

    public function test_preg_replace_expression_on_valid_strings(): void
    {
        $pregReplace = regex_replace(lit('/(foo)/'), lit('bar'), lit('foo is awesome'));

        static::assertSame('bar is awesome', $pregReplace->eval(row(), flow_context()));
    }
}

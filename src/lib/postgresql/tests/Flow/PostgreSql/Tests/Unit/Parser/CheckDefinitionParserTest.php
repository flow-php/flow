<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Parser\{CheckDefinitionParser, ExpressionParser};
use PHPUnit\Framework\TestCase;

final class CheckDefinitionParserTest extends TestCase
{
    private CheckDefinitionParser $parser;

    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }

        $this->parser = new CheckDefinitionParser(new ExpressionParser(new Parser()));
    }

    public function test_does_not_strip_when_no_closing_paren() : void
    {
        $this->expectException(\Throwable::class);

        $this->parser->parse('CHECK (a > 0');
    }

    public function test_leading_whitespace_tolerated() : void
    {
        self::assertSame('a > 0', $this->parser->parse('  CHECK (a > 0)'));
    }

    public function test_normalizes_implicit_casts() : void
    {
        self::assertSame(
            "status = 'active'",
            $this->parser->parse("CHECK (((status)::text = 'active'::text))")
        );
    }

    public function test_passes_through_unwrapped_expression() : void
    {
        self::assertSame('a > 0', $this->parser->parse('a > 0'));
    }

    public function test_strips_check_wrapper_when_present() : void
    {
        self::assertSame('a > 0', $this->parser->parse('CHECK (a > 0)'));
    }

    public function test_wrapper_detection_is_case_insensitive() : void
    {
        self::assertSame('a > 0', $this->parser->parse('check (a > 0)'));
    }
}

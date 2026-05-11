<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Parser;

use Flow\PostgreSql\Parser\ExpressionParser;
use PHPUnit\Framework\TestCase;

final class ExpressionParserTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_normalize_generation_expression_preserves_expression_without_casts(): void
    {
        $parser = new ExpressionParser();

        static::assertSame("(first_name || ' ') || last_name", $parser->normalize("first_name || ' ' || last_name"));
    }

    public function test_normalize_generation_expression_strips_casts_from_concatenation(): void
    {
        $parser = new ExpressionParser();

        static::assertSame(
            "(first_name || ' ') || last_name",
            $parser->normalize("(first_name)::text || ' '::text || (last_name)::text"),
        );
    }

    public function test_normalize_generation_expression_strips_casts_from_function_arguments(): void
    {
        $parser = new ExpressionParser();

        static::assertSame(
            "setweight(to_tsvector('english', name), 'A')",
            $parser->normalize("setweight(to_tsvector('english'::regconfig, name::text), 'A'::\"char\")"),
        );
    }

    public function test_normalize_generation_expression_strips_nested_casts(): void
    {
        $parser = new ExpressionParser();

        static::assertSame('upper(name)', $parser->normalize('upper((name)::text)'));
    }

    public function test_normalize_generation_expression_strips_simple_column_cast(): void
    {
        $parser = new ExpressionParser();

        static::assertSame('name', $parser->normalize('(name)::text'));
    }
}

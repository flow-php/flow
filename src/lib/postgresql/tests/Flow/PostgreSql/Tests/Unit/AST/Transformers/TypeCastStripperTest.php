<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\TypeCastStripper;
use Flow\PostgreSql\Parser\ExpressionParser;
use PHPUnit\Framework\TestCase;

final class TypeCastStripperTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_strip_array_expression() : void
    {
        self::assertSame(
            "ARRAY['a', 'b']",
            $this->normalize("ARRAY['a'::text, 'b'::text]"),
        );
    }

    public function test_strip_bool_expression_and() : void
    {
        self::assertSame(
            'name = 1 AND status = 2',
            $this->normalize('name::int = 1 AND status::int = 2'),
        );
    }

    public function test_strip_case_expression_branches() : void
    {
        self::assertSame(
            "CASE WHEN name IS NULL THEN 'n/a' ELSE name END",
            $this->normalize("CASE WHEN name IS NULL THEN 'n/a'::text ELSE (name)::text END"),
        );
    }

    public function test_strip_coalesce_arguments() : void
    {
        self::assertSame(
            "COALESCE(name, 'n/a')",
            $this->normalize("COALESCE((name)::text, 'n/a'::text)"),
        );
    }

    public function test_strip_leaves_expression_without_casts_untouched() : void
    {
        self::assertSame(
            "(first_name || ' ') || last_name",
            $this->normalize("first_name || ' ' || last_name"),
        );
    }

    public function test_strip_nested_casts_in_function_call() : void
    {
        self::assertSame(
            "setweight(to_tsvector('english', name), 'A')",
            $this->normalize("setweight(to_tsvector('english'::regconfig, name::text), 'A'::\"char\")"),
        );
    }

    public function test_strip_null_test_argument() : void
    {
        self::assertSame(
            'name IS NULL',
            $this->normalize('(name)::text IS NULL'),
        );
    }

    public function test_strip_row_expression_elements() : void
    {
        self::assertSame(
            "ROW(1, 'a')",
            $this->normalize("ROW(1::int, 'a'::text)"),
        );
    }

    public function test_strip_stacked_casts() : void
    {
        self::assertSame(
            'name',
            $this->normalize('((name)::text)::varchar'),
        );
    }

    public function test_strip_top_level_cast() : void
    {
        self::assertSame(
            'name',
            $this->normalize('(name)::text'),
        );
    }

    private function normalize(string $expression) : string
    {
        // Routes through ExpressionParser::normalize(), which uses TypeCastStripper.
        return (new ExpressionParser(new TypeCastStripper()))->normalize($expression);
    }
}

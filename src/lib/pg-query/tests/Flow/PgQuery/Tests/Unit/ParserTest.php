<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit;

use Flow\PgQuery\{ParsedQuery, Parser};
use PHPUnit\Framework\TestCase;

final class ParserTest extends TestCase
{
    protected function setUp() : void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_deparse_complex_query() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $sql = 'SELECT u.id, u.name, COUNT(*) AS total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id, u.name ORDER BY total DESC LIMIT 10';
        $parsed = $parser->parse($sql);

        $deparsed = $parsed->deparse();

        self::assertNotEmpty($deparsed);

        $reparsed = $parser->parse($deparsed);
        $redeparsed = $reparsed->deparse();
        self::assertSame($deparsed, $redeparsed);
    }

    public function test_deparse_round_trip() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $parsed = $parser->parse('SELECT id FROM users WHERE name = \'john\'');
        $deparsed = $parsed->deparse();

        $reparsed = $parser->parse($deparsed);
        $redeparsed = $reparsed->deparse();

        self::assertSame($deparsed, $redeparsed);
    }

    public function test_deparse_select_with_columns() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $parsed = $parser->parse('SELECT id, name FROM users');

        $deparsed = $parsed->deparse();

        self::assertSame('SELECT id, name FROM users', $deparsed);
    }

    public function test_deparse_select_with_where() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users WHERE active = true');

        $deparsed = $parsed->deparse();

        self::assertSame('SELECT * FROM users WHERE active = true', $deparsed);
    }

    public function test_deparse_simple_select() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $parsed = $parser->parse('SELECT 1');

        $deparsed = $parsed->deparse();

        self::assertSame('SELECT 1', $deparsed);
    }

    public function test_fingerprint() : void
    {
        $parser = new Parser();
        $fingerprint = $parser->fingerprint('SELECT 1');

        self::assertIsString($fingerprint);
        self::assertNotEmpty($fingerprint);
    }

    public function test_fingerprint_same_for_equivalent_queries() : void
    {
        $parser = new Parser();

        $fingerprint1 = $parser->fingerprint('SELECT id FROM users WHERE id = 1');
        $fingerprint2 = $parser->fingerprint('SELECT id FROM users WHERE id = 2');

        self::assertSame($fingerprint1, $fingerprint2);
    }

    public function test_normalize() : void
    {
        $parser = new Parser();
        $normalized = $parser->normalize('SELECT * FROM users WHERE id = 1');

        self::assertIsString($normalized);
        self::assertStringContainsString('$1', $normalized);
    }

    public function test_normalize_multiple_values() : void
    {
        $parser = new Parser();
        $normalized = $parser->normalize("SELECT * FROM users WHERE id = 1 AND name = 'john'");

        self::assertIsString($normalized);
        self::assertStringContainsString('$1', $normalized);
        self::assertStringContainsString('$2', $normalized);
    }

    public function test_normalize_with_named_parameters() : void
    {
        $parser = new Parser();
        $normalized = $parser->normalize('SELECT * FROM users WHERE id = :id AND name = :name');

        self::assertIsString($normalized);
        self::assertStringContainsString('$1', $normalized);
        self::assertStringContainsString('$2', $normalized);
    }

    public function test_parse_invalid_sql_throws_exception() : void
    {
        $parser = new Parser();

        $this->expectException(\Flow\PgQuery\Exception\ParserException::class);
        $this->expectExceptionMessage('syntax error');

        $parser->parse('SELECT FROM WHERE');
    }

    public function test_parse_multiple_statements() : void
    {
        $parser = new Parser();
        $result = $parser->parse('SELECT 1; SELECT 2');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(2, $result->raw()->getStmts());
    }

    public function test_parse_select_with_columns() : void
    {
        $parser = new Parser();
        $result = $parser->parse('SELECT id, name FROM users');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(1, $result->raw()->getStmts());
    }

    public function test_parse_select_with_where() : void
    {
        $parser = new Parser();
        $result = $parser->parse('SELECT * FROM users WHERE active = true');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(1, $result->raw()->getStmts());
    }

    public function test_parse_simple_select() : void
    {
        $parser = new Parser();
        $result = $parser->parse('SELECT 1');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(1, $result->raw()->getStmts());
    }

    public function test_split() : void
    {
        $parser = new Parser();
        $statements = $parser->split('SELECT 1; SELECT 2;');

        self::assertCount(2, $statements);
        self::assertSame('SELECT 1', $statements[0]);
        self::assertSame(' SELECT 2', $statements[1]);
    }

    public function test_split_single_statement() : void
    {
        $parser = new Parser();
        $statements = $parser->split('SELECT 1');

        self::assertCount(1, $statements);
        self::assertSame('SELECT 1', $statements[0]);
    }
}

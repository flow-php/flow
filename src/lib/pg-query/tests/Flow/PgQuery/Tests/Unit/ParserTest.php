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

    public function test_normalize_utility() : void
    {
        if (!\function_exists('pg_query_normalize_utility')) {
            self::markTestSkipped('pg_query_normalize_utility function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $normalized = $parser->normalizeUtility('CREATE TABLE users (id INT, name VARCHAR(255))');

        self::assertIsString($normalized);
        self::assertStringContainsString('CREATE TABLE', $normalized);
        self::assertStringContainsString('users', $normalized);
    }

    public function test_normalize_utility_preserves_ddl_structure() : void
    {
        if (!\function_exists('pg_query_normalize_utility')) {
            self::markTestSkipped('pg_query_normalize_utility function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $normalized = $parser->normalizeUtility('ALTER TABLE users ADD COLUMN email VARCHAR(255)');

        self::assertIsString($normalized);
        self::assertStringContainsString('ALTER TABLE', $normalized);
        self::assertStringContainsString('users', $normalized);
        self::assertStringContainsString('email', $normalized);
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

    public function test_summary_returns_protobuf_for_select() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('SELECT * FROM users WHERE id = 1');

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
        self::assertGreaterThan(0, \strlen($summary));
    }

    public function test_summary_returns_protobuf_for_insert() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary("INSERT INTO users (name, email) VALUES ('john', 'john@example.com')");

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_update() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary("UPDATE users SET name = 'jane' WHERE id = 1");

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_delete() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('DELETE FROM users WHERE id = 1');

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_join_query() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id');

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_subquery() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_cte() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users');

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_ddl() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))');

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_with_parse_options() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summary = $parser->summary('SELECT 1', PG_QUERY_PARSE_DEFAULT);

        self::assertIsString($summary);
        self::assertNotEmpty($summary);
    }

    public function test_summary_with_truncation_returns_different_result() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $longQuery = "SELECT * FROM users WHERE name = 'this is a very long string that should be truncated in the summary output'";

        $summaryWithoutTruncation = $parser->summary($longQuery, 0, 0);
        $summaryWithTruncation = $parser->summary($longQuery, 0, 20);

        self::assertIsString($summaryWithTruncation);
        self::assertNotEmpty($summaryWithTruncation);
        self::assertNotSame($summaryWithoutTruncation, $summaryWithTruncation);
    }

    public function test_summary_different_queries_produce_different_results() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();
        $summarySelect = $parser->summary('SELECT * FROM users');
        $summaryInsert = $parser->summary("INSERT INTO users (name) VALUES ('john')");

        self::assertNotSame($summarySelect, $summaryInsert);
    }

    public function test_summary_invalid_sql_throws_parser_exception() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();

        $this->expectException(\Flow\PgQuery\Exception\ParserException::class);

        $parser->summary('SELECT FROM WHERE');
    }

    public function test_summary_empty_query_throws_parser_exception() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $parser = new Parser();

        $this->expectException(\Flow\PgQuery\Exception\ParserException::class);

        $parser->summary('');
    }
}

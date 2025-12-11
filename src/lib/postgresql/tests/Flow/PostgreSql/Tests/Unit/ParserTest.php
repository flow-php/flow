<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit;

use function Flow\PostgreSql\DSL\{sql_deparse, sql_deparse_options, sql_fingerprint, sql_format, sql_normalize, sql_normalize_utility, sql_parse, sql_parser, sql_split, sql_summary};
use Flow\PostgreSql\ParsedQuery;
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

        $parser = sql_parser();
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

        $parser = sql_parser();
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

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT id, name FROM users');

        $deparsed = $parsed->deparse();

        self::assertSame('SELECT id, name FROM users', $deparsed);
    }

    public function test_deparse_select_with_where() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT * FROM users WHERE active = true');

        $deparsed = $parsed->deparse();

        self::assertSame('SELECT * FROM users WHERE active = true', $deparsed);
    }

    public function test_deparse_simple_select() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT 1');

        $deparsed = $parsed->deparse();

        self::assertSame('SELECT 1', $deparsed);
    }

    public function test_deparse_with_options_complex_query() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $sql = 'SELECT u.id, u.name, COUNT(*) AS total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id, u.name HAVING COUNT(*) > 5 ORDER BY total DESC LIMIT 10';
        $parsed = $parser->parse($sql);

        $formatted = $parsed->deparse(sql_deparse_options()->indentSize(2));

        $expected = <<<'SQL'
SELECT u.id, u.name, count(*) AS total
FROM
  users u
  JOIN orders o ON u.id = o.user_id
WHERE u.active = true
GROUP BY u.id, u.name
HAVING count(*) > 5
ORDER BY total DESC
LIMIT 10
SQL;

        self::assertSame($expected, $formatted);

        $reparsed = $parser->parse($formatted);
        self::assertCount(1, $reparsed->raw()->getStmts());
    }

    public function test_deparse_with_options_create_table() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('CREATE TABLE users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL, email VARCHAR(255) UNIQUE, created_at TIMESTAMP DEFAULT NOW())');

        $formatted = $parsed->deparse(sql_deparse_options());

        self::assertStringContainsString('CREATE TABLE users', $formatted);
        self::assertStringContainsString('id serial', \strtolower($formatted));
        self::assertStringContainsString('name', $formatted);
        self::assertStringContainsString('email', $formatted);
    }

    public function test_deparse_with_options_custom_indent_size() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT u.id FROM users u JOIN orders o ON u.id = o.user_id');

        $formatted = $parsed->deparse(sql_deparse_options()->indentSize(2));

        $expected = <<<'SQL'
SELECT u.id
FROM
  users u
  JOIN orders o ON u.id = o.user_id
SQL;

        self::assertSame($expected, $formatted);
    }

    public function test_deparse_with_options_insert() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse("INSERT INTO users (name, email, active) VALUES ('john', 'john@example.com', true)");

        $formatted = $parsed->deparse(sql_deparse_options());

        $expected = <<<'SQL'
INSERT INTO users (name, email, active)
VALUES ('john', 'john@example.com', true)
SQL;

        self::assertSame($expected, $formatted);
    }

    public function test_deparse_with_options_pretty_print_disabled_equals_regular_deparse() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT id FROM users');

        $regular = $parsed->deparse();
        $formatted = $parsed->deparse(sql_deparse_options()->prettyPrint(false));

        self::assertSame($regular, $formatted);
    }

    public function test_deparse_with_options_pretty_prints_join() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT u.id, u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true');

        $formatted = $parsed->deparse(sql_deparse_options());

        $expected = <<<'SQL'
SELECT u.id, u.name, o.total
FROM
    users u
    JOIN orders o ON u.id = o.user_id
WHERE u.active = true
SQL;

        self::assertSame($expected, $formatted);
    }

    public function test_deparse_with_options_pretty_prints_simple_select() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT id, name, email FROM users WHERE active = true');

        $formatted = $parsed->deparse(sql_deparse_options());

        $expected = <<<'SQL'
SELECT id, name, email
FROM users
WHERE active = true
SQL;

        self::assertSame($expected, $formatted);
    }

    public function test_deparse_with_options_trailing_newline() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parser = sql_parser();
        $parsed = $parser->parse('SELECT 1');

        $formatted = $parsed->deparse(sql_deparse_options()->trailingNewline());

        self::assertSame("SELECT 1\n", $formatted);
    }

    public function test_fingerprint() : void
    {
        $fingerprint = sql_fingerprint('SELECT 1');

        self::assertIsString($fingerprint);
        self::assertNotEmpty($fingerprint);
    }

    public function test_fingerprint_same_for_equivalent_queries() : void
    {
        $fingerprint1 = sql_fingerprint('SELECT id FROM users WHERE id = 1');
        $fingerprint2 = sql_fingerprint('SELECT id FROM users WHERE id = 2');

        self::assertSame($fingerprint1, $fingerprint2);
    }

    public function test_normalize() : void
    {
        $normalized = sql_normalize('SELECT * FROM users WHERE id = 1');

        self::assertIsString($normalized);
        self::assertStringContainsString('$1', $normalized);
    }

    public function test_normalize_multiple_values() : void
    {
        $normalized = sql_normalize("SELECT * FROM users WHERE id = 1 AND name = 'john'");

        self::assertIsString($normalized);
        self::assertStringContainsString('$1', $normalized);
        self::assertStringContainsString('$2', $normalized);
    }

    public function test_normalize_utility() : void
    {
        if (!\function_exists('pg_query_normalize_utility')) {
            self::markTestSkipped('pg_query_normalize_utility function not available. Rebuild the pg_query extension.');
        }

        $normalized = sql_normalize_utility('CREATE TABLE users (id INT, name VARCHAR(255))');

        self::assertIsString($normalized);
        self::assertStringContainsString('CREATE TABLE', $normalized);
        self::assertStringContainsString('users', $normalized);
    }

    public function test_normalize_utility_preserves_ddl_structure() : void
    {
        if (!\function_exists('pg_query_normalize_utility')) {
            self::markTestSkipped('pg_query_normalize_utility function not available. Rebuild the pg_query extension.');
        }

        $normalized = sql_normalize_utility('ALTER TABLE users ADD COLUMN email VARCHAR(255)');

        self::assertIsString($normalized);
        self::assertStringContainsString('ALTER TABLE', $normalized);
        self::assertStringContainsString('users', $normalized);
        self::assertStringContainsString('email', $normalized);
    }

    public function test_normalize_with_named_parameters() : void
    {
        $normalized = sql_normalize('SELECT * FROM users WHERE id = :id AND name = :name');

        self::assertIsString($normalized);
        self::assertStringContainsString('$1', $normalized);
        self::assertStringContainsString('$2', $normalized);
    }

    public function test_parse_invalid_sql_throws_exception() : void
    {
        $parser = sql_parser();

        $this->expectException(\Flow\PostgreSql\Exception\ParserException::class);
        $this->expectExceptionMessage('syntax error');

        $parser->parse('SELECT FROM WHERE');
    }

    public function test_parse_multiple_statements() : void
    {
        $parser = sql_parser();
        $result = $parser->parse('SELECT 1; SELECT 2');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(2, $result->raw()->getStmts());
    }

    public function test_parse_select_with_columns() : void
    {
        $parser = sql_parser();
        $result = $parser->parse('SELECT id, name FROM users');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(1, $result->raw()->getStmts());
    }

    public function test_parse_select_with_where() : void
    {
        $parser = sql_parser();
        $result = $parser->parse('SELECT * FROM users WHERE active = true');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(1, $result->raw()->getStmts());
    }

    public function test_parse_simple_select() : void
    {
        $parser = sql_parser();
        $result = $parser->parse('SELECT 1');

        self::assertInstanceOf(ParsedQuery::class, $result);
        self::assertCount(1, $result->raw()->getStmts());
    }

    public function test_pg_deparse_with_options() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $parsed = sql_parse('SELECT id FROM users u JOIN orders o ON u.id = o.user_id');

        $deparsed = sql_deparse($parsed, sql_deparse_options()->indentSize(2));

        $expected = <<<'SQL'
SELECT id
FROM
  users u
  JOIN orders o ON u.id = o.user_id
SQL;

        self::assertSame($expected, $deparsed);
    }

    public function test_pg_deparse_without_options() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $parsed = sql_parse('SELECT id, name FROM users');

        $deparsed = sql_deparse($parsed);

        self::assertSame('SELECT id, name FROM users', $deparsed);
    }

    public function test_pg_format_with_custom_options() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $formatted = sql_format('SELECT 1', sql_deparse_options()->trailingNewline());

        self::assertSame("SELECT 1\n", $formatted);
    }

    public function test_pg_format_with_default_options() : void
    {
        if (!\function_exists('pg_query_deparse_opts')) {
            self::markTestSkipped('pg_query_deparse_opts function not available. Rebuild the pg_query extension.');
        }

        $formatted = sql_format('SELECT id, name FROM users WHERE active = true');

        $expected = <<<'SQL'
SELECT id, name
FROM users
WHERE active = true
SQL;

        self::assertSame($expected, $formatted);
    }

    public function test_split() : void
    {
        $statements = sql_split('SELECT 1; SELECT 2;');

        self::assertCount(2, $statements);
        self::assertSame('SELECT 1', $statements[0]);
        self::assertSame(' SELECT 2', $statements[1]);
    }

    public function test_split_single_statement() : void
    {
        $statements = sql_split('SELECT 1');

        self::assertCount(1, $statements);
        self::assertSame('SELECT 1', $statements[0]);
    }

    public function test_summary_different_queries_produce_different_results() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summarySelect = sql_summary('SELECT * FROM users');
        $summaryInsert = sql_summary("INSERT INTO users (name) VALUES ('john')");

        self::assertNotSame($summarySelect, $summaryInsert);
    }

    public function test_summary_invalid_sql_throws_parser_exception() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $this->expectException(\Flow\PostgreSql\Exception\ParserException::class);

        sql_summary('SELECT FROM WHERE');
    }

    public function test_summary_returns_protobuf_for_cte() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('WITH active_users AS (SELECT * FROM users WHERE active = true) SELECT * FROM active_users');

        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_ddl() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(255))');

        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_delete() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('DELETE FROM users WHERE id = 1');

        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_insert() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary("INSERT INTO users (name, email) VALUES ('john', 'john@example.com')");

        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_join_query() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id');

        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_select() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('SELECT * FROM users WHERE id = 1');

        self::assertNotEmpty($summary);
        self::assertGreaterThan(0, \strlen($summary));
    }

    public function test_summary_returns_protobuf_for_subquery() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');

        self::assertNotEmpty($summary);
    }

    public function test_summary_returns_protobuf_for_update() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary("UPDATE users SET name = 'jane' WHERE id = 1");

        self::assertNotEmpty($summary);
    }

    public function test_summary_with_parse_options() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $summary = sql_summary('SELECT 1', PG_QUERY_PARSE_DEFAULT);

        self::assertNotEmpty($summary);
    }

    public function test_summary_with_truncation_returns_different_result() : void
    {
        if (!\function_exists('pg_query_summary')) {
            self::markTestSkipped('pg_query_summary function not available. Rebuild the pg_query extension.');
        }

        $longQuery = "SELECT * FROM users WHERE name = 'this is a very long string that should be truncated in the summary output'";

        $summaryWithoutTruncation = sql_summary($longQuery, 0, 0);
        $summaryWithTruncation = sql_summary($longQuery, 0, 20);

        self::assertNotEmpty($summaryWithTruncation);
        self::assertNotSame($summaryWithoutTruncation, $summaryWithTruncation);
    }
}

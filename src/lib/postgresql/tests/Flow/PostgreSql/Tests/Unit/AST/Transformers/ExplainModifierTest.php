<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Nodes\Exception\InvalidStatementException;
use Flow\PostgreSql\AST\Transformers\ExplainConfig;
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_explain_config;
use function Flow\PostgreSql\DSL\sql_explain_modifier;
use function Flow\PostgreSql\DSL\sql_parse;
use function Flow\PostgreSql\DSL\sql_to_explain;
use function sprintf;

final class ExplainModifierTest extends TestCase
{
    public static function explainableStatements(): Generator
    {
        yield 'INSERT' => ['INSERT INTO t (a) VALUES (1)'];
        yield 'UPDATE' => ['UPDATE t SET a = 1'];
        yield 'DELETE' => ['DELETE FROM t'];
        yield 'MERGE' => ['MERGE INTO t USING u ON t.id = u.id WHEN MATCHED THEN DELETE'];
        yield 'CREATE TABLE AS' => ['CREATE TABLE x AS SELECT 1'];
        yield 'EXECUTE' => ['EXECUTE p(1)'];
        yield 'DECLARE' => ['DECLARE c CURSOR FOR SELECT 1'];
    }

    public static function nonExplainableStatements(): Generator
    {
        yield 'CREATE TABLE' => ['CREATE TABLE x (a int)', 'create_stmt'];
        yield 'REFRESH MATERIALIZED VIEW' => ['REFRESH MATERIALIZED VIEW mv', 'refresh_mat_view_stmt'];
        yield 'EXPLAIN' => ['EXPLAIN SELECT 1', 'explain_stmt'];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_analyze_config_includes_all_options(): void
    {
        $config = ExplainConfig::forAnalysis();

        static::assertTrue($config->analyze);
        static::assertTrue($config->costs);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertSame(ExplainFormat::JSON, $config->format);
    }

    public function test_config_with_all_options_in_constructor(): void
    {
        $config = new ExplainConfig(
            analyze: true,
            verbose: true,
            costs: true,
            buffers: true,
            timing: true,
            summary: true,
            memory: true,
            settings: true,
            wal: true,
            format: ExplainFormat::JSON,
        );

        static::assertTrue($config->analyze);
        static::assertTrue($config->verbose);
        static::assertTrue($config->costs);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertTrue($config->summary);
        static::assertTrue($config->memory);
        static::assertTrue($config->settings);
        static::assertTrue($config->wal);
        static::assertSame(ExplainFormat::JSON, $config->format);
    }

    public function test_custom_config(): void
    {
        $config = sql_explain_config(
            analyze: true,
            verbose: true,
            costs: true,
            buffers: true,
            timing: true,
            format: ExplainFormat::TEXT,
        );

        static::assertTrue($config->analyze);
        static::assertTrue($config->verbose);
        static::assertTrue($config->costs);
        static::assertTrue($config->buffers);
        static::assertTrue($config->timing);
        static::assertSame(ExplainFormat::TEXT, $config->format);
    }

    #[DataProvider('nonExplainableStatements')]
    public function test_rejects_non_explainable_statement(string $sql, string $statement): void
    {
        $this->expectException(InvalidStatementException::class);
        $this->expectExceptionMessage(sprintf('EXPLAIN cannot explain a "%s" statement', $statement));

        sql_to_explain($sql);
    }

    public function test_rejects_before_mutating_any_statement(): void
    {
        $query = sql_parse('SELECT 1; TRUNCATE t');

        try {
            $query->traverse(sql_explain_modifier(ExplainConfig::forAnalysis()));
            static::fail('TRUNCATE must not be explained');
        } catch (InvalidStatementException) {
        }

        static::assertSame('SELECT 1; TRUNCATE t', $query->deparse());
    }

    public function test_estimate_config(): void
    {
        $config = ExplainConfig::forEstimate();

        static::assertFalse($config->analyze);
        static::assertTrue($config->costs);
        static::assertFalse($config->buffers);
        static::assertSame(ExplainFormat::JSON, $config->format);
    }

    public function test_estimate_config_generates_explain_without_analyze(): void
    {
        $result = sql_to_explain('SELECT id FROM orders', ExplainConfig::forEstimate());

        static::assertSame('EXPLAIN (COSTS 1, FORMAT "json") SELECT id FROM orders', $result);
    }

    public function test_explains_complex_query(): void
    {
        $sql = <<<'SQL'
            SELECT u.id, u.name, COUNT(o.id) as order_count
            FROM users u
            LEFT JOIN orders o ON u.id = o.user_id
            WHERE u.active = true
            GROUP BY u.id, u.name
            HAVING COUNT(o.id) > 5
            ORDER BY order_count DESC
            LIMIT 10
            SQL;

        $result = sql_to_explain($sql);

        static::assertSame(
            'EXPLAIN (ANALYZE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT "json") SELECT u.id, u.name, count(o.id) AS order_count FROM users u LEFT JOIN orders o ON u.id = o.user_id WHERE u.active = true GROUP BY u.id, u.name HAVING count(o.id) > 5 ORDER BY order_count DESC LIMIT 10',
            $result,
        );
    }

    public function test_format_text(): void
    {
        $config = sql_explain_config(format: ExplainFormat::TEXT);
        $result = sql_to_explain('SELECT * FROM users', $config);

        static::assertSame(
            'EXPLAIN (ANALYZE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT text) SELECT * FROM users',
            $result,
        );
    }

    public function test_format_xml(): void
    {
        $config = sql_explain_config(format: ExplainFormat::XML);
        $result = sql_to_explain('SELECT * FROM users', $config);

        static::assertSame(
            'EXPLAIN (ANALYZE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT xml) SELECT * FROM users',
            $result,
        );
    }

    public function test_format_yaml(): void
    {
        $config = sql_explain_config(format: ExplainFormat::YAML);
        $result = sql_to_explain('SELECT * FROM users', $config);

        static::assertSame(
            'EXPLAIN (ANALYZE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT yaml) SELECT * FROM users',
            $result,
        );
    }

    public function test_modifier_with_analyze_verbose_and_buffers(): void
    {
        $config = sql_explain_config(analyze: true, verbose: true, buffers: true);
        $result = sql_to_explain('SELECT * FROM users', $config);

        static::assertSame(
            'EXPLAIN (ANALYZE, VERBOSE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT "json") SELECT * FROM users',
            $result,
        );
    }

    public function test_only_modifies_top_level_statement(): void
    {
        $result = sql_to_explain('SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)');

        static::assertSame(
            'EXPLAIN (ANALYZE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT "json") SELECT * FROM users WHERE id IN (SELECT user_id FROM orders)',
            $result,
        );
    }

    #[DataProvider('explainableStatements')]
    public function test_wraps_every_explainable_statement(string $sql): void
    {
        static::assertStringStartsWith('EXPLAIN (', sql_to_explain($sql));
    }

    public function test_wraps_each_statement(): void
    {
        static::assertSame('EXPLAIN (COSTS 1, FORMAT "json") SELECT 1; EXPLAIN (COSTS 1, FORMAT "json") SELECT 2', sql_to_explain(
            'SELECT 1; SELECT 2',
            ExplainConfig::forEstimate(),
        ));
    }

    public function test_wraps_select_with_explain_analyze(): void
    {
        $result = sql_to_explain('SELECT * FROM users');

        static::assertSame(
            'EXPLAIN (ANALYZE, COSTS 1, BUFFERS 1, TIMING 1, SUMMARY 1, FORMAT "json") SELECT * FROM users',
            $result,
        );
    }

    public function test_wraps_select_with_explain_estimate(): void
    {
        $result = sql_to_explain('SELECT * FROM users', ExplainConfig::forEstimate());

        static::assertSame('EXPLAIN (COSTS 1, FORMAT "json") SELECT * FROM users', $result);
    }
}

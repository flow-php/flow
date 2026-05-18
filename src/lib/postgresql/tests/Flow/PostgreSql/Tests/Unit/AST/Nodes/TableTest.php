<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Table;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function pg_query_parse;

final class TableTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_alias_returns_alias_name(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM users AS u');

        $table = new Table($rangeVar);

        static::assertSame('u', $table->alias());
    }

    public function test_alias_returns_null_when_no_alias(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM users');

        $table = new Table($rangeVar);

        static::assertNull($table->alias());
    }

    public function test_name_returns_table_name(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM users');

        $table = new Table($rangeVar);

        static::assertSame('users', $table->name());
    }

    public function test_name_returns_table_name_for_schema_qualified_table(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM public.users');

        $table = new Table($rangeVar);

        static::assertSame('users', $table->name());
    }

    public function test_raw_returns_underlying_range_var(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM users');

        $table = new Table($rangeVar);

        static::assertSame($rangeVar, $table->raw());
    }

    public function test_schema_returns_null_for_unqualified_table(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM users');

        $table = new Table($rangeVar);

        static::assertNull($table->schema());
    }

    public function test_schema_returns_schema_name_for_qualified_table(): void
    {
        $rangeVar = $this->extractRangeVar('SELECT * FROM public.users');

        $table = new Table($rangeVar);

        static::assertSame('public', $table->schema());
    }

    private function extractRangeVar(string $sql): RangeVar
    {
        $parseResult = $this->parseQuery($sql);
        $stmts = $parseResult->getStmts();

        $stmt = $stmts[0]->getStmt();
        self::assertNotNull($stmt);

        $selectStmt = $stmt->getSelectStmt();
        self::assertNotNull($selectStmt);

        $fromClause = $selectStmt->getFromClause();
        $rangeVar = $fromClause[0]->getRangeVar();
        self::assertNotNull($rangeVar);

        return $rangeVar;
    }

    private function parseQuery(string $sql): ParseResult
    {
        $json = pg_query_parse($sql);
        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return $result;
    }
}

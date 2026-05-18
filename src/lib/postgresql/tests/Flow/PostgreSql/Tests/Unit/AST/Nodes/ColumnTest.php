<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Column;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function pg_query_parse;

final class ColumnTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_name_returns_column_name(): void
    {
        $columnRef = $this->extractColumnRef('SELECT id FROM users');

        $column = new Column($columnRef);

        static::assertSame('id', $column->name());
    }

    public function test_name_returns_star_for_table_qualified_wildcard(): void
    {
        $columnRef = $this->extractColumnRef('SELECT u.* FROM users u');

        $column = new Column($columnRef);

        static::assertSame('*', $column->name());
    }

    public function test_name_returns_star_for_wildcard(): void
    {
        $columnRef = $this->extractColumnRef('SELECT * FROM users');

        $column = new Column($columnRef);

        static::assertSame('*', $column->name());
    }

    public function test_raw_returns_underlying_column_ref(): void
    {
        $columnRef = $this->extractColumnRef('SELECT id FROM users');

        $column = new Column($columnRef);

        static::assertSame($columnRef, $column->raw());
    }

    public function test_table_returns_null_for_unqualified_column(): void
    {
        $columnRef = $this->extractColumnRef('SELECT id FROM users');

        $column = new Column($columnRef);

        static::assertNull($column->table());
    }

    public function test_table_returns_null_for_unqualified_wildcard(): void
    {
        $columnRef = $this->extractColumnRef('SELECT * FROM users');

        $column = new Column($columnRef);

        static::assertNull($column->table());
    }

    public function test_table_returns_table_name_for_qualified_column(): void
    {
        $columnRef = $this->extractColumnRef('SELECT u.id FROM users u');

        $column = new Column($columnRef);

        static::assertSame('u', $column->table());
    }

    public function test_table_returns_table_name_for_qualified_wildcard(): void
    {
        $columnRef = $this->extractColumnRef('SELECT u.* FROM users u');

        $column = new Column($columnRef);

        static::assertSame('u', $column->table());
    }

    private function extractColumnRef(string $sql): ColumnRef
    {
        $parseResult = $this->parseQuery($sql);
        $stmts = $parseResult->getStmts();

        $stmt = $stmts[0]->getStmt();
        self::assertNotNull($stmt);

        $selectStmt = $stmt->getSelectStmt();
        self::assertNotNull($selectStmt);

        $targetList = $selectStmt->getTargetList();
        $resTarget = $targetList[0]->getResTarget();
        self::assertNotNull($resTarget);

        $val = $resTarget->getVal();
        self::assertNotNull($val);

        $columnRef = $val->getColumnRef();
        self::assertNotNull($columnRef);

        return $columnRef;
    }

    private function parseQuery(string $sql): ParseResult
    {
        $json = pg_query_parse($sql);
        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return $result;
    }
}

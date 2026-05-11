<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\FunctionCall;
use Flow\PostgreSql\Protobuf\AST\FuncCall;
use Flow\PostgreSql\Protobuf\AST\ParseResult;
use PHPUnit\Framework\TestCase;

final class FunctionCallTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_name_returns_function_name(): void
    {
        $funcCall = $this->extractFuncCall('SELECT count(*) FROM users');

        $function = new FunctionCall($funcCall);

        static::assertSame('count', $function->name());
    }

    public function test_name_returns_function_name_for_schema_qualified_function(): void
    {
        $funcCall = $this->extractFuncCall('SELECT pg_catalog.now()');

        $function = new FunctionCall($funcCall);

        static::assertSame('now', $function->name());
    }

    public function test_raw_returns_underlying_func_call(): void
    {
        $funcCall = $this->extractFuncCall('SELECT count(*) FROM users');

        $function = new FunctionCall($funcCall);

        static::assertSame($funcCall, $function->raw());
    }

    public function test_schema_returns_null_for_unqualified_function(): void
    {
        $funcCall = $this->extractFuncCall('SELECT count(*) FROM users');

        $function = new FunctionCall($funcCall);

        static::assertNull($function->schema());
    }

    public function test_schema_returns_schema_name_for_qualified_function(): void
    {
        $funcCall = $this->extractFuncCall('SELECT pg_catalog.now()');

        $function = new FunctionCall($funcCall);

        static::assertSame('pg_catalog', $function->schema());
    }

    private function extractFuncCall(string $sql): FuncCall
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

        $funcCall = $val->getFuncCall();
        self::assertNotNull($funcCall);

        return $funcCall;
    }

    private function parseQuery(string $sql): ParseResult
    {
        /** @var string $json */
        $json = \pg_query_parse($sql);
        $result = new ParseResult();
        $result->mergeFromJsonString($json);

        return $result;
    }
}

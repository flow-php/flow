<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Transaction;

use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{Node, RawStmt, TransactionStmt, TransactionStmtKind};
use Flow\PostgreSql\QueryBuilder\Transaction\SavepointBuilder;
use PHPUnit\Framework\TestCase;

final class SavepointBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_release_savepoint_ast_has_correct_kind() : void
    {
        $query = SavepointBuilder::release('my_savepoint');

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_RELEASE, $ast->getKind());
        self::assertSame('my_savepoint', $ast->getSavepointName());
    }

    public function test_release_savepoint_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SavepointBuilder::release('my_savepoint');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('RELEASE my_savepoint', $deparsed);
    }

    public function test_savepoint_ast_has_correct_kind() : void
    {
        $query = SavepointBuilder::create('my_savepoint');

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_SAVEPOINT, $ast->getKind());
        self::assertSame('my_savepoint', $ast->getSavepointName());
    }

    public function test_savepoint_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = SavepointBuilder::create('my_savepoint');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('SAVEPOINT my_savepoint', $deparsed);
    }

    private function deparse(TransactionStmt $stmt) : string
    {
        $parser = new Parser();
        $node = new Node();
        $node->setTransactionStmt($stmt);
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}

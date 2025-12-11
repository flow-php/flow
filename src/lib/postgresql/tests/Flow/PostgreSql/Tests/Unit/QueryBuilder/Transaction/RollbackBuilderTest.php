<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Transaction;

use Flow\PostgreSql\{ParsedQuery, Parser};
use Flow\PostgreSql\Protobuf\AST\{Node, RawStmt, TransactionStmt, TransactionStmtKind};
use Flow\PostgreSql\QueryBuilder\Transaction\RollbackBuilder;
use PHPUnit\Framework\TestCase;

final class RollbackBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_immutability() : void
    {
        $original = RollbackBuilder::create();
        $modified = $original->toSavepoint('test');

        self::assertNotSame($original, $modified);
    }

    public function test_rollback_ast_has_correct_kind() : void
    {
        $query = RollbackBuilder::create();

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_ROLLBACK, $ast->getKind());
    }

    public function test_rollback_deparsed_basic() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = RollbackBuilder::create();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('ROLLBACK', $deparsed);
    }

    public function test_rollback_deparsed_to_savepoint() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = RollbackBuilder::create()
            ->toSavepoint('my_savepoint');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('ROLLBACK TO SAVEPOINT my_savepoint', $deparsed);
    }

    public function test_rollback_deparsed_with_and_chain() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = RollbackBuilder::create()
            ->andChain();

        $deparsed = $this->deparse($query->toAst());
        self::assertSame('ROLLBACK AND CHAIN', $deparsed);
    }

    public function test_rollback_to_savepoint_has_correct_kind() : void
    {
        $query = RollbackBuilder::create()
            ->toSavepoint('my_savepoint');

        $ast = $query->toAst();

        self::assertSame(TransactionStmtKind::TRANS_STMT_ROLLBACK_TO, $ast->getKind());
        self::assertSame('my_savepoint', $ast->getSavepointName());
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

<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Transaction;

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{Node, RawStmt, TransactionStmt, TransactionStmtKind};
use Flow\PgQuery\QueryBuilder\Transaction\PreparedTransactionBuilder;
use PHPUnit\Framework\TestCase;

final class PreparedTransactionBuilderTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_commit_prepared_ast_has_correct_kind() : void
    {
        $query = PreparedTransactionBuilder::commitPrepared('my_transaction');

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_COMMIT_PREPARED, $ast->getKind());
        self::assertSame('my_transaction', $ast->getGid());
    }

    public function test_commit_prepared_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = PreparedTransactionBuilder::commitPrepared('my_transaction');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("COMMIT PREPARED 'my_transaction'", $deparsed);
    }

    public function test_prepare_transaction_ast_has_correct_kind() : void
    {
        $query = PreparedTransactionBuilder::prepare('my_transaction');

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_PREPARE, $ast->getKind());
        self::assertSame('my_transaction', $ast->getGid());
    }

    public function test_prepare_transaction_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = PreparedTransactionBuilder::prepare('my_transaction');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("PREPARE TRANSACTION 'my_transaction'", $deparsed);
    }

    public function test_rollback_prepared_ast_has_correct_kind() : void
    {
        $query = PreparedTransactionBuilder::rollbackPrepared('my_transaction');

        $ast = $query->toAst();

        self::assertInstanceOf(TransactionStmt::class, $ast);
        self::assertSame(TransactionStmtKind::TRANS_STMT_ROLLBACK_PREPARED, $ast->getKind());
        self::assertSame('my_transaction', $ast->getGid());
    }

    public function test_rollback_prepared_deparsed() : void
    {
        if (!\function_exists('pg_query_deparse')) {
            self::markTestSkipped('pg_query_deparse function not available. Rebuild the pg_query extension.');
        }

        $query = PreparedTransactionBuilder::rollbackPrepared('my_transaction');

        $deparsed = $this->deparse($query->toAst());
        self::assertSame("ROLLBACK PREPARED 'my_transaction'", $deparsed);
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

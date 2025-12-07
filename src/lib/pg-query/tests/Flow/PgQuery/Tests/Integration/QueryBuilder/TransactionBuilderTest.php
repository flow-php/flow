<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    begin,
    commit,
    commit_prepared,
    prepare_transaction,
    release_savepoint,
    rollback,
    rollback_prepared,
    savepoint,
    set_session_transaction,
    set_transaction,
    start_transaction,
    transaction_snapshot
};

use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{Node, RawStmt, TransactionStmt, VariableSetStmt};
use Flow\PgQuery\QueryBuilder\Transaction\{
    BeginFinalStep,
    CommitFinalStep,
    IsolationLevel,
    PreparedTransactionFinalStep,
    RollbackFinalStep,
    SavepointFinalStep,
    SetTransactionFinalStep
};

final class TransactionBuilderTest extends PGQueryTestCase
{
    // -------------------------------------------------------------------
    // BEGIN / START TRANSACTION
    // -------------------------------------------------------------------

    public function test_begin_basic() : void
    {
        $query = begin();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN'
        );
    }

    public function test_begin_with_all_options() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::SERIALIZABLE)
            ->readOnly()
            ->deferrable();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE'
        );
    }

    public function test_begin_with_deferrable() : void
    {
        $query = begin()
            ->deferrable();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN DEFERRABLE'
        );
    }

    public function test_begin_with_isolation_level_read_committed() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::READ_COMMITTED);

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN ISOLATION LEVEL READ COMMITTED'
        );
    }

    public function test_begin_with_isolation_level_read_uncommitted() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::READ_UNCOMMITTED);

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN ISOLATION LEVEL READ UNCOMMITTED'
        );
    }

    public function test_begin_with_isolation_level_repeatable_read() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::REPEATABLE_READ);

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN ISOLATION LEVEL REPEATABLE READ'
        );
    }

    public function test_begin_with_isolation_level_serializable() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN ISOLATION LEVEL SERIALIZABLE'
        );
    }

    public function test_begin_with_not_deferrable() : void
    {
        $query = begin()
            ->notDeferrable();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN NOT DEFERRABLE'
        );
    }

    public function test_begin_with_read_only() : void
    {
        $query = begin()
            ->readOnly();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN READ ONLY'
        );
    }

    public function test_begin_with_read_write() : void
    {
        $query = begin()
            ->readWrite();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN READ WRITE'
        );
    }

    public function test_commit_and_chain() : void
    {
        $query = commit()
            ->andChain();

        $this->assertTransactionQueryEquals(
            $query,
            'COMMIT AND CHAIN'
        );
    }

    // -------------------------------------------------------------------
    // COMMIT
    // -------------------------------------------------------------------

    public function test_commit_basic() : void
    {
        $query = commit();

        $this->assertTransactionQueryEquals(
            $query,
            'COMMIT'
        );
    }

    public function test_commit_prepared() : void
    {
        $query = commit_prepared('my_transaction');

        $this->assertTransactionQueryEquals(
            $query,
            "COMMIT PREPARED 'my_transaction'"
        );
    }

    // -------------------------------------------------------------------
    // PREPARED TRANSACTIONS (TWO-PHASE COMMIT)
    // -------------------------------------------------------------------

    public function test_prepare_transaction() : void
    {
        $query = prepare_transaction('my_transaction');

        $this->assertTransactionQueryEquals(
            $query,
            "PREPARE TRANSACTION 'my_transaction'"
        );
    }

    public function test_release_savepoint() : void
    {
        $query = release_savepoint('my_savepoint');

        $this->assertTransactionQueryEquals(
            $query,
            'RELEASE my_savepoint'
        );
    }

    public function test_rollback_and_chain() : void
    {
        $query = rollback()
            ->andChain();

        $this->assertTransactionQueryEquals(
            $query,
            'ROLLBACK AND CHAIN'
        );
    }

    // -------------------------------------------------------------------
    // ROLLBACK
    // -------------------------------------------------------------------

    public function test_rollback_basic() : void
    {
        $query = rollback();

        $this->assertTransactionQueryEquals(
            $query,
            'ROLLBACK'
        );
    }

    public function test_rollback_prepared() : void
    {
        $query = rollback_prepared('my_transaction');

        $this->assertTransactionQueryEquals(
            $query,
            "ROLLBACK PREPARED 'my_transaction'"
        );
    }

    public function test_rollback_to_savepoint() : void
    {
        $query = rollback()
            ->toSavepoint('my_savepoint');

        $this->assertTransactionQueryEquals(
            $query,
            'ROLLBACK TO SAVEPOINT my_savepoint'
        );
    }

    // -------------------------------------------------------------------
    // SAVEPOINT
    // -------------------------------------------------------------------

    public function test_savepoint() : void
    {
        $query = savepoint('my_savepoint');

        $this->assertTransactionQueryEquals(
            $query,
            'SAVEPOINT my_savepoint'
        );
    }

    public function test_set_session_transaction() : void
    {
        $query = set_session_transaction()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        $this->assertSetTransactionQueryEquals(
            $query,
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE'
        );
    }

    public function test_set_transaction_deferrable() : void
    {
        $query = set_transaction()
            ->deferrable();

        $this->assertSetTransactionQueryEquals(
            $query,
            'SET TRANSACTION DEFERRABLE'
        );
    }

    // -------------------------------------------------------------------
    // SET TRANSACTION
    // -------------------------------------------------------------------

    public function test_set_transaction_isolation_level() : void
    {
        $query = set_transaction()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        $this->assertSetTransactionQueryEquals(
            $query,
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE'
        );
    }

    public function test_set_transaction_read_only() : void
    {
        $query = set_transaction()
            ->readOnly();

        $this->assertSetTransactionQueryEquals(
            $query,
            'SET TRANSACTION READ ONLY'
        );
    }

    public function test_set_transaction_read_write() : void
    {
        $query = set_transaction()
            ->readWrite();

        $this->assertSetTransactionQueryEquals(
            $query,
            'SET TRANSACTION READ WRITE'
        );
    }

    public function test_set_transaction_with_multiple_options() : void
    {
        $query = set_transaction()
            ->isolationLevel(IsolationLevel::SERIALIZABLE)
            ->readOnly()
            ->deferrable();

        $this->assertSetTransactionQueryEquals(
            $query,
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE'
        );
    }

    public function test_start_transaction_basic() : void
    {
        $query = start_transaction();

        $this->assertTransactionQueryEquals(
            $query,
            'BEGIN'
        );
    }

    public function test_transaction_snapshot() : void
    {
        $query = transaction_snapshot('00000003-0000001A-1');

        $this->assertSetTransactionQueryEquals(
            $query,
            "SET TRANSACTION SNAPSHOT '00000003-0000001A-1'"
        );
    }

    protected function assertSetTransactionQueryEquals(SetTransactionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseVariableSetStmt($builder->toAst());

        self::assertSame($expectedSql, $sql);
    }

    // -------------------------------------------------------------------
    // Assertions
    // -------------------------------------------------------------------

    protected function assertTransactionQueryEquals(BeginFinalStep|CommitFinalStep|RollbackFinalStep|SavepointFinalStep|PreparedTransactionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseTransactionStmt($builder->toAst());

        self::assertSame($expectedSql, $sql);
    }

    protected function deparseTransactionStmt(TransactionStmt $stmt) : string
    {
        $node = new Node();
        $node->setTransactionStmt($stmt);

        $parser = new Parser();
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }

    protected function deparseVariableSetStmt(VariableSetStmt $stmt) : string
    {
        $node = new Node();
        $node->setVariableSetStmt($stmt);

        $parser = new Parser();
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}

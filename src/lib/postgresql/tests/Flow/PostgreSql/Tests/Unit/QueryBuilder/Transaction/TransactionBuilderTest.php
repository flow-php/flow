<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Transaction;

use function Flow\PostgreSql\DSL\{
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
    transaction_snapshot
};

use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use PHPUnit\Framework\TestCase;

final class TransactionBuilderTest extends TestCase
{
    public function test_begin_basic() : void
    {
        $query = begin();

        self::assertSame(
            'BEGIN',
            $query->toSql()
        );
    }

    public function test_begin_with_all_options() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::SERIALIZABLE)
            ->readOnly()
            ->deferrable();

        self::assertSame(
            'BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE',
            $query->toSql()
        );
    }

    public function test_begin_with_deferrable() : void
    {
        $query = begin()
            ->deferrable();

        self::assertSame(
            'BEGIN DEFERRABLE',
            $query->toSql()
        );
    }

    public function test_begin_with_isolation_level_read_committed() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::READ_COMMITTED);

        self::assertSame(
            'BEGIN ISOLATION LEVEL READ COMMITTED',
            $query->toSql()
        );
    }

    public function test_begin_with_isolation_level_read_uncommitted() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::READ_UNCOMMITTED);

        self::assertSame(
            'BEGIN ISOLATION LEVEL READ UNCOMMITTED',
            $query->toSql()
        );
    }

    public function test_begin_with_isolation_level_repeatable_read() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::REPEATABLE_READ);

        self::assertSame(
            'BEGIN ISOLATION LEVEL REPEATABLE READ',
            $query->toSql()
        );
    }

    public function test_begin_with_isolation_level_serializable() : void
    {
        $query = begin()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        self::assertSame(
            'BEGIN ISOLATION LEVEL SERIALIZABLE',
            $query->toSql()
        );
    }

    public function test_begin_with_not_deferrable() : void
    {
        $query = begin()
            ->notDeferrable();

        self::assertSame(
            'BEGIN NOT DEFERRABLE',
            $query->toSql()
        );
    }

    public function test_begin_with_read_only() : void
    {
        $query = begin()
            ->readOnly();

        self::assertSame(
            'BEGIN READ ONLY',
            $query->toSql()
        );
    }

    public function test_begin_with_read_write() : void
    {
        $query = begin()
            ->readWrite();

        self::assertSame(
            'BEGIN READ WRITE',
            $query->toSql()
        );
    }

    public function test_commit_and_chain() : void
    {
        $query = commit()
            ->andChain();

        self::assertSame(
            'COMMIT AND CHAIN',
            $query->toSql()
        );
    }

    public function test_commit_basic() : void
    {
        $query = commit();

        self::assertSame(
            'COMMIT',
            $query->toSql()
        );
    }

    public function test_commit_prepared() : void
    {
        $query = commit_prepared('my_transaction');

        self::assertSame(
            "COMMIT PREPARED 'my_transaction'",
            $query->toSql()
        );
    }

    public function test_prepare_transaction() : void
    {
        $query = prepare_transaction('my_transaction');

        self::assertSame(
            "PREPARE TRANSACTION 'my_transaction'",
            $query->toSql()
        );
    }

    public function test_release_savepoint() : void
    {
        $query = release_savepoint('my_savepoint');

        self::assertSame(
            'RELEASE my_savepoint',
            $query->toSql()
        );
    }

    public function test_rollback_and_chain() : void
    {
        $query = rollback()
            ->andChain();

        self::assertSame(
            'ROLLBACK AND CHAIN',
            $query->toSql()
        );
    }

    public function test_rollback_basic() : void
    {
        $query = rollback();

        self::assertSame(
            'ROLLBACK',
            $query->toSql()
        );
    }

    public function test_rollback_prepared() : void
    {
        $query = rollback_prepared('my_transaction');

        self::assertSame(
            "ROLLBACK PREPARED 'my_transaction'",
            $query->toSql()
        );
    }

    public function test_rollback_to_savepoint() : void
    {
        $query = rollback()
            ->toSavepoint('my_savepoint');

        self::assertSame(
            'ROLLBACK TO SAVEPOINT my_savepoint',
            $query->toSql()
        );
    }

    public function test_savepoint() : void
    {
        $query = savepoint('my_savepoint');

        self::assertSame(
            'SAVEPOINT my_savepoint',
            $query->toSql()
        );
    }

    public function test_set_session_transaction() : void
    {
        $query = set_session_transaction()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        self::assertSame(
            'SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE',
            $query->toSql()
        );
    }

    public function test_set_transaction_deferrable() : void
    {
        $query = set_transaction()
            ->deferrable();

        self::assertSame(
            'SET TRANSACTION DEFERRABLE',
            $query->toSql()
        );
    }

    public function test_set_transaction_isolation_level() : void
    {
        $query = set_transaction()
            ->isolationLevel(IsolationLevel::SERIALIZABLE);

        self::assertSame(
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE',
            $query->toSql()
        );
    }

    public function test_set_transaction_read_only() : void
    {
        $query = set_transaction()
            ->readOnly();

        self::assertSame(
            'SET TRANSACTION READ ONLY',
            $query->toSql()
        );
    }

    public function test_set_transaction_read_write() : void
    {
        $query = set_transaction()
            ->readWrite();

        self::assertSame(
            'SET TRANSACTION READ WRITE',
            $query->toSql()
        );
    }

    public function test_set_transaction_with_multiple_options() : void
    {
        $query = set_transaction()
            ->isolationLevel(IsolationLevel::SERIALIZABLE)
            ->readOnly()
            ->deferrable();

        self::assertSame(
            'SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE',
            $query->toSql()
        );
    }

    public function test_start_transaction_basic() : void
    {
        $query = begin();

        self::assertSame(
            'BEGIN',
            $query->toSql()
        );
    }

    public function test_transaction_snapshot() : void
    {
        $query = transaction_snapshot('00000003-0000001A-1');

        self::assertSame(
            "SET TRANSACTION SNAPSHOT '00000003-0000001A-1'",
            $query->toSql()
        );
    }
}

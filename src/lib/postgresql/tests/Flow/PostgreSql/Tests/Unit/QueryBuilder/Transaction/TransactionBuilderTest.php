<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Transaction;

use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\begin;
use function Flow\PostgreSql\DSL\commit;
use function Flow\PostgreSql\DSL\commit_prepared;
use function Flow\PostgreSql\DSL\prepare_transaction;
use function Flow\PostgreSql\DSL\release_savepoint;
use function Flow\PostgreSql\DSL\rollback;
use function Flow\PostgreSql\DSL\rollback_prepared;
use function Flow\PostgreSql\DSL\savepoint;
use function Flow\PostgreSql\DSL\set_session_transaction;
use function Flow\PostgreSql\DSL\set_transaction;
use function Flow\PostgreSql\DSL\transaction_snapshot;

final class TransactionBuilderTest extends TestCase
{
    public function test_begin_basic(): void
    {
        $query = begin();

        static::assertSame('BEGIN', $query->toSql());
    }

    public function test_begin_with_all_options(): void
    {
        $query = begin()->isolationLevel(IsolationLevel::SERIALIZABLE)->readOnly()->deferrable();

        static::assertSame('BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE', $query->toSql());
    }

    public function test_begin_with_deferrable(): void
    {
        $query = begin()->deferrable();

        static::assertSame('BEGIN DEFERRABLE', $query->toSql());
    }

    public function test_begin_with_isolation_level_read_committed(): void
    {
        $query = begin()->isolationLevel(IsolationLevel::READ_COMMITTED);

        static::assertSame('BEGIN ISOLATION LEVEL READ COMMITTED', $query->toSql());
    }

    public function test_begin_with_isolation_level_read_uncommitted(): void
    {
        $query = begin()->isolationLevel(IsolationLevel::READ_UNCOMMITTED);

        static::assertSame('BEGIN ISOLATION LEVEL READ UNCOMMITTED', $query->toSql());
    }

    public function test_begin_with_isolation_level_repeatable_read(): void
    {
        $query = begin()->isolationLevel(IsolationLevel::REPEATABLE_READ);

        static::assertSame('BEGIN ISOLATION LEVEL REPEATABLE READ', $query->toSql());
    }

    public function test_begin_with_isolation_level_serializable(): void
    {
        $query = begin()->isolationLevel(IsolationLevel::SERIALIZABLE);

        static::assertSame('BEGIN ISOLATION LEVEL SERIALIZABLE', $query->toSql());
    }

    public function test_begin_with_not_deferrable(): void
    {
        $query = begin()->notDeferrable();

        static::assertSame('BEGIN NOT DEFERRABLE', $query->toSql());
    }

    public function test_begin_with_read_only(): void
    {
        $query = begin()->readOnly();

        static::assertSame('BEGIN READ ONLY', $query->toSql());
    }

    public function test_begin_with_read_write(): void
    {
        $query = begin()->readWrite();

        static::assertSame('BEGIN READ WRITE', $query->toSql());
    }

    public function test_commit_and_chain(): void
    {
        $query = commit()->andChain();

        static::assertSame('COMMIT AND CHAIN', $query->toSql());
    }

    public function test_commit_basic(): void
    {
        $query = commit();

        static::assertSame('COMMIT', $query->toSql());
    }

    public function test_commit_prepared(): void
    {
        $query = commit_prepared('my_transaction');

        static::assertSame("COMMIT PREPARED 'my_transaction'", $query->toSql());
    }

    public function test_prepare_transaction(): void
    {
        $query = prepare_transaction('my_transaction');

        static::assertSame("PREPARE TRANSACTION 'my_transaction'", $query->toSql());
    }

    public function test_release_savepoint(): void
    {
        $query = release_savepoint('my_savepoint');

        static::assertSame('RELEASE my_savepoint', $query->toSql());
    }

    public function test_rollback_and_chain(): void
    {
        $query = rollback()->andChain();

        static::assertSame('ROLLBACK AND CHAIN', $query->toSql());
    }

    public function test_rollback_basic(): void
    {
        $query = rollback();

        static::assertSame('ROLLBACK', $query->toSql());
    }

    public function test_rollback_prepared(): void
    {
        $query = rollback_prepared('my_transaction');

        static::assertSame("ROLLBACK PREPARED 'my_transaction'", $query->toSql());
    }

    public function test_rollback_to_savepoint(): void
    {
        $query = rollback()->toSavepoint('my_savepoint');

        static::assertSame('ROLLBACK TO SAVEPOINT my_savepoint', $query->toSql());
    }

    public function test_savepoint(): void
    {
        $query = savepoint('my_savepoint');

        static::assertSame('SAVEPOINT my_savepoint', $query->toSql());
    }

    public function test_set_session_transaction(): void
    {
        $query = set_session_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE);

        static::assertSame('SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE', $query->toSql());
    }

    public function test_set_transaction_deferrable(): void
    {
        $query = set_transaction()->deferrable();

        static::assertSame('SET TRANSACTION DEFERRABLE', $query->toSql());
    }

    public function test_set_transaction_isolation_level(): void
    {
        $query = set_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE);

        static::assertSame('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE', $query->toSql());
    }

    public function test_set_transaction_read_only(): void
    {
        $query = set_transaction()->readOnly();

        static::assertSame('SET TRANSACTION READ ONLY', $query->toSql());
    }

    public function test_set_transaction_read_write(): void
    {
        $query = set_transaction()->readWrite();

        static::assertSame('SET TRANSACTION READ WRITE', $query->toSql());
    }

    public function test_set_transaction_with_multiple_options(): void
    {
        $query = set_transaction()->isolationLevel(IsolationLevel::SERIALIZABLE)->readOnly()->deferrable();

        static::assertSame('SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE', $query->toSql());
    }

    public function test_start_transaction_basic(): void
    {
        $query = begin();

        static::assertSame('BEGIN', $query->toSql());
    }

    public function test_transaction_snapshot(): void
    {
        $query = transaction_snapshot('00000003-0000001A-1');

        static::assertSame("SET TRANSACTION SNAPSHOT '00000003-0000001A-1'", $query->toSql());
    }
}

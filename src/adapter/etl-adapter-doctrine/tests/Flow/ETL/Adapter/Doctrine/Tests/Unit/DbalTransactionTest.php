<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Exception as DbalException;
use Doctrine\DBAL\TransactionIsolationLevel;
use Flow\ETL\Adapter\Doctrine\DbalTransaction;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class DbalTransactionTest extends TestCase
{
    public function test_the_connection_is_opened_lazily_on_begin(): void
    {
        $transaction = new DbalTransaction(['driver' => 'not_a_driver']);

        $this->expectException(DbalException::class);

        $transaction->begin();
    }

    public function test_begin_leaves_the_isolation_level_alone_when_none_is_set(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('getParams')->willReturn([]);
        $connection->expects(self::once())->method('beginTransaction');
        $connection->expects(self::never())->method('setTransactionIsolation');

        DbalTransaction::fromConnection($connection)->begin();
    }

    public function test_the_isolation_level_is_set_before_begin_and_restored_after_commit(): void
    {
        $levels = [];
        $connection = $this->createStub(Connection::class);
        $connection->method('getParams')->willReturn([]);
        $connection->method('getTransactionIsolation')->willReturn(TransactionIsolationLevel::READ_COMMITTED);
        $connection
            ->method('setTransactionIsolation')
            ->willReturnCallback(static function (TransactionIsolationLevel $level) use (&$levels): void {
                $levels[] = $level;
            });
        $transaction = DbalTransaction::fromConnection(
            $connection,
        )->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE);

        $transaction->begin();
        $transaction->commit();

        static::assertSame(
            [TransactionIsolationLevel::SERIALIZABLE, TransactionIsolationLevel::READ_COMMITTED],
            $levels,
        );
    }

    public function test_begin_restores_the_isolation_level_when_begin_throws(): void
    {
        $levels = [];
        $failure = new RuntimeException('begin failed');
        $connection = $this->createStub(Connection::class);
        $connection->method('getParams')->willReturn([]);
        $connection->method('getTransactionIsolation')->willReturn(TransactionIsolationLevel::READ_COMMITTED);
        $connection->method('beginTransaction')->willThrowException($failure);
        $connection
            ->method('setTransactionIsolation')
            ->willReturnCallback(static function (TransactionIsolationLevel $level) use (&$levels): void {
                $levels[] = $level;
            });

        try {
            DbalTransaction::fromConnection($connection)
                ->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE)
                ->begin();
            static::fail('begin() must rethrow');
        } catch (RuntimeException $thrown) {
            static::assertSame($failure, $thrown);
        }

        static::assertSame(
            [TransactionIsolationLevel::SERIALIZABLE, TransactionIsolationLevel::READ_COMMITTED],
            $levels,
        );
    }

    public function test_a_failed_commit_leaves_the_restore_to_the_rollback_that_follows(): void
    {
        $levels = [];
        $connection = $this->createStub(Connection::class);
        $connection->method('getParams')->willReturn([]);
        $connection->method('getTransactionIsolation')->willReturn(TransactionIsolationLevel::READ_COMMITTED);
        $connection->method('commit')->willThrowException(new RuntimeException('commit failed'));
        $connection
            ->method('setTransactionIsolation')
            ->willReturnCallback(static function (TransactionIsolationLevel $level) use (&$levels): void {
                $levels[] = $level;
            });
        $transaction = DbalTransaction::fromConnection(
            $connection,
        )->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE);
        $transaction->begin();

        try {
            $transaction->commit();
            static::fail('commit() must rethrow the commit failure');
        } catch (RuntimeException $failure) {
            static::assertSame([TransactionIsolationLevel::SERIALIZABLE], $levels);

            $transaction->rollback($failure);
        }

        static::assertSame(
            [TransactionIsolationLevel::SERIALIZABLE, TransactionIsolationLevel::READ_COMMITTED],
            $levels,
        );
    }

    public function test_rollback_rolls_back_and_suppresses_its_own_failure(): void
    {
        $connection = $this->createMock(Connection::class);
        $connection->method('getParams')->willReturn([]);
        $connection
            ->expects(self::once())
            ->method('rollBack')
            ->willThrowException(new RuntimeException('rollback failed'));

        DbalTransaction::fromConnection($connection)->rollback(new RuntimeException('load failed'));
    }

    public function test_with_isolation_level_returns_a_new_instance(): void
    {
        $transaction = new DbalTransaction(['driver' => 'pdo_sqlite', 'memory' => true]);

        static::assertNotSame($transaction, $transaction->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE));
    }
}

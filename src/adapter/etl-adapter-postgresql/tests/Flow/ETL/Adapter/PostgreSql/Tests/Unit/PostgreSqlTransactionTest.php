<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\PostgreSqlTransaction;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function str_contains;

final class PostgreSqlTransactionTest extends TestCase
{
    public function test_begin_opens_a_transaction_without_setting_an_isolation_level(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->expects(self::never())->method('execute');

        (new PostgreSqlTransaction($client))->begin();
    }

    public function test_begin_sets_the_isolation_level_inside_the_transaction(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client
            ->expects(self::once())
            ->method('execute')
            ->with(static::callback(
                static fn(Sql|string $sql): bool => $sql instanceof Sql
                && str_contains($sql->toSql(), 'ISOLATION LEVEL SERIALIZABLE'),
            ))
            ->willReturn(0);

        (new PostgreSqlTransaction($client))
            ->withIsolationLevel(IsolationLevel::SERIALIZABLE)
            ->begin();
    }

    public function test_a_failing_isolation_level_closes_the_transaction_begin_opened(): void
    {
        $failure = new RuntimeException('set failed');
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->method('execute')->willThrowException($failure);
        $client->expects(self::once())->method('rollBack');

        $this->expectExceptionObject($failure);

        (new PostgreSqlTransaction($client))
            ->withIsolationLevel(IsolationLevel::SERIALIZABLE)
            ->begin();
    }

    public function test_commit_commits(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('commit');

        (new PostgreSqlTransaction($client))->commit();
    }

    public function test_rollback_rolls_back_and_suppresses_its_own_failure(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('rollBack')->willThrowException(new RuntimeException('rollback failed'));

        (new PostgreSqlTransaction($client))->rollback(new RuntimeException('load failed'));
    }

    public function test_with_isolation_level_returns_a_new_instance(): void
    {
        $transaction = new PostgreSqlTransaction($this->createStub(Client::class));

        static::assertNotSame($transaction, $transaction->withIsolationLevel(IsolationLevel::SERIALIZABLE));
    }
}

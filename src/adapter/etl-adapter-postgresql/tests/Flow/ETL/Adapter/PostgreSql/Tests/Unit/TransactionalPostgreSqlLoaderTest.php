<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\TransactionalPostgreSqlLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Loader;
use Flow\ETL\Tests\Double\ClosureThrowingLoader;
use Flow\ETL\Tests\Double\SpyLoader;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;
use PHPUnit\Framework\TestCase;
use RuntimeException;

use function Flow\ETL\DSL\flow_context;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function str_contains;

final class TransactionalPostgreSqlLoaderTest extends TestCase
{
    public function test_closure_is_forwarded_to_every_closure_aware_loader_inside_a_transaction(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->expects(self::once())->method('commit');
        $client->expects(self::never())->method('rollBack');

        $context = flow_context();
        $spy1 = new SpyLoader();
        $spy2 = new SpyLoader();

        (new TransactionalPostgreSqlLoader($client, $spy1, $this->createStub(Loader::class), $spy2))->closure($context);

        static::assertSame(1, $spy1->closureCount);
        static::assertSame(1, $spy2->closureCount);
        static::assertSame([$context], $spy1->closureContexts);
        static::assertSame([$context], $spy2->closureContexts);
    }

    public function test_closure_rolls_back_and_rethrows_when_a_forwarded_closure_fails(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->expects(self::never())->method('commit');
        $client->expects(self::once())->method('rollBack');

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('closure failed');

        (new TransactionalPostgreSqlLoader(
            $client,
            new ClosureThrowingLoader(new RuntimeException('closure failed')),
        ))->closure(flow_context());
    }

    public function test_the_original_failure_propagates_when_rollback_also_fails(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->expects(self::never())->method('commit');
        $client->expects(self::once())->method('rollBack')->willThrowException(new RuntimeException('rollback failed'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('closure failed');

        (new TransactionalPostgreSqlLoader(
            $client,
            new ClosureThrowingLoader(new RuntimeException('closure failed')),
        ))->closure(flow_context());
    }

    public function test_closure_sets_isolation_level_inside_its_transaction(): void
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
        $client->expects(self::once())->method('commit');

        (new TransactionalPostgreSqlLoader($client, new SpyLoader()))
            ->withIsolationLevel(IsolationLevel::SERIALIZABLE)
            ->closure(flow_context());
    }

    public function test_commits_after_running_every_loader(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->expects(self::once())->method('commit');
        $client->expects(self::never())->method('rollBack');

        $rows = rows(row(int_entry('id', 1)));

        $first = $this->createMock(Loader::class);
        $first->expects(self::once())->method('load')->with($rows);
        $second = $this->createMock(Loader::class);
        $second->expects(self::once())->method('load')->with($rows);

        (new TransactionalPostgreSqlLoader($client, $first, $second))->load($rows, flow_context());
    }

    public function test_rolls_back_and_rethrows_when_a_loader_fails(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::once())->method('beginTransaction');
        $client->expects(self::never())->method('commit');
        $client->expects(self::once())->method('rollBack');

        $failing = $this->createStub(Loader::class);
        $failing->method('load')->willThrowException(new RuntimeException('loader failed'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('loader failed');

        (new TransactionalPostgreSqlLoader($client, $failing))->load(rows(row(int_entry('id', 1))), flow_context());
    }

    public function test_exposing_the_wrapped_loaders(): void
    {
        $first = $this->createStub(Loader::class);
        $second = $this->createStub(Loader::class);

        static::assertSame(
            [$first, $second],
            (new TransactionalPostgreSqlLoader($this->createStub(Client::class), $first, $second))->loaders(),
        );
    }

    public function test_requires_at_least_one_loader(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one loader must be provided');

        new TransactionalPostgreSqlLoader($this->createStub(Client::class));
    }

    public function test_sets_isolation_level_before_running_loaders(): void
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
        $client->expects(self::once())->method('commit');

        $loader = $this->createMock(Loader::class);
        $loader->expects(self::once())->method('load');

        (new TransactionalPostgreSqlLoader($client, $loader))
            ->withIsolationLevel(IsolationLevel::SERIALIZABLE)
            ->load(rows(row(int_entry('id', 1))), flow_context());
    }

    public function test_skips_transaction_for_empty_rows(): void
    {
        $client = $this->createMock(Client::class);
        $client->expects(self::never())->method('beginTransaction');
        $client->expects(self::never())->method('commit');
        $client->expects(self::never())->method('rollBack');

        $loader = $this->createMock(Loader::class);
        $loader->expects(self::never())->method('load');

        (new TransactionalPostgreSqlLoader($client, $loader))->load(rows(), flow_context());
    }
}

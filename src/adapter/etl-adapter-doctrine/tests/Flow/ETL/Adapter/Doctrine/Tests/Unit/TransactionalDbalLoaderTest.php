<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\TransactionIsolationLevel;
use Flow\ETL\Adapter\Doctrine\DbalLoader;
use Flow\ETL\Adapter\Doctrine\TransactionalDbalLoader;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\Double\SpyLoader;
use PHPUnit\Framework\TestCase;

use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;

final class TransactionalDbalLoaderTest extends TestCase
{
    public function test_accepts_multiple_dbal_loaders(): void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader1 = new DbalLoader('test_table1', $params);
        $loader2 = new DbalLoader('test_table2', $params);

        $transactionalLoader = new TransactionalDbalLoader($params, $loader1, $loader2);

        static::assertInstanceOf(TransactionalDbalLoader::class, $transactionalLoader);
    }

    public function test_closure_is_forwarded_to_every_closure_aware_loader(): void
    {
        $context = flow_context(config());
        $spy1 = new SpyLoader();
        $spy2 = new SpyLoader();

        (new TransactionalDbalLoader(
            ['driver' => 'pdo_sqlite', 'memory' => true],
            $spy1,
            new DbalLoader('test_table', ['driver' => 'pdo_sqlite', 'memory' => true]),
            $spy2,
        ))->closure($context);

        static::assertSame(1, $spy1->closureCount);
        static::assertSame(1, $spy2->closureCount);
        static::assertSame([$context], $spy1->closureContexts);
        static::assertSame([$context], $spy2->closureContexts);
    }

    public function test_connection_from_params(): void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader = new DbalLoader('test_table', $params);

        $transactionalLoader = new TransactionalDbalLoader($params, $loader);

        static::assertInstanceOf(TransactionalDbalLoader::class, $transactionalLoader);
    }

    public function test_exposing_the_wrapped_loaders(): void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader1 = new DbalLoader('test_table1', $params);
        $loader2 = new DbalLoader('test_table2', $params);

        static::assertSame([$loader1, $loader2], (new TransactionalDbalLoader($params, $loader1, $loader2))->loaders());
    }

    public function test_from_connection_static_method(): void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $connection = $this->createStub(Connection::class);
        $connection->method('getParams')->willReturn($params);

        $loader = new DbalLoader('test_table', $params);
        $transactionalLoader = TransactionalDbalLoader::fromConnection($connection, $loader);

        static::assertInstanceOf(TransactionalDbalLoader::class, $transactionalLoader);
    }

    public function test_requires_at_least_one_loader(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one loader must be provided');

        new TransactionalDbalLoader([]);
    }

    public function test_sets_isolation_level(): void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader = new DbalLoader('test_table', $params);

        $transactionalLoader = new TransactionalDbalLoader($params, $loader);
        $result = $transactionalLoader->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE);

        static::assertSame($transactionalLoader, $result);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit;

use Doctrine\DBAL\{Connection, TransactionIsolationLevel};
use Flow\ETL\Adapter\Doctrine\{DbalLoader, TransactionalDbalLoader};
use Flow\ETL\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class TransactionalDbalLoaderTest extends TestCase
{
    public function test_accepts_multiple_dbal_loaders() : void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader1 = new DbalLoader('test_table1', $params);
        $loader2 = new DbalLoader('test_table2', $params);

        $transactionalLoader = new TransactionalDbalLoader($params, $loader1, $loader2);

        self::assertInstanceOf(TransactionalDbalLoader::class, $transactionalLoader);
    }

    public function test_connection_from_params() : void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader = new DbalLoader('test_table', $params);

        $transactionalLoader = new TransactionalDbalLoader($params, $loader);

        self::assertInstanceOf(TransactionalDbalLoader::class, $transactionalLoader);
    }

    public function test_from_connection_static_method() : void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $connection = $this->createMock(Connection::class);
        $connection->method('getParams')->willReturn($params);

        $loader = new DbalLoader('test_table', $params);
        $transactionalLoader = TransactionalDbalLoader::fromConnection($connection, $loader);

        self::assertInstanceOf(TransactionalDbalLoader::class, $transactionalLoader);
    }

    public function test_requires_at_least_one_loader() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one loader must be provided');

        new TransactionalDbalLoader([]);
    }

    public function test_sets_isolation_level() : void
    {
        $params = ['driver' => 'pdo_sqlite', 'memory' => true];
        $loader = new DbalLoader('test_table', $params);

        $transactionalLoader = new TransactionalDbalLoader($params, $loader);
        $result = $transactionalLoader->withIsolationLevel(TransactionIsolationLevel::SERIALIZABLE);

        self::assertSame($transactionalLoader, $result);
    }
}

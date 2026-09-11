<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Context;

use Doctrine\DBAL\Configuration;
use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Driver\Middleware;
use Doctrine\DBAL\DriverManager;
use PDO;
use RuntimeException;
use SQLite3;

final readonly class InMemorySqlite
{
    public static function connection(Middleware ...$middlewares): Connection
    {
        return DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ], (new Configuration())->setMiddlewares($middlewares));
    }

    /**
     * The native handle of a pdo_sqlite connection, as DbalResultSchema's dispatch sees it.
     */
    public static function pdo(int $users = 0): PDO
    {
        $connection = self::connection();

        if ($users > 0) {
            self::withUsers($connection, $users);
        }

        $native = $connection->getNativeConnection();

        return $native instanceof PDO ? $native : throw new RuntimeException('pdo_sqlite did not hand out a PDO');
    }

    /**
     * A bare SQLite3 handle with exceptions enabled, as DBAL's sqlite3 driver hands them out.
     */
    public static function sqlite3(): SQLite3
    {
        $sqlite = new SQLite3(':memory:');
        $sqlite->enableExceptions(true);

        return $sqlite;
    }

    public static function withUsers(Connection $connection, int $count): Connection
    {
        $connection->executeStatement('CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT NOT NULL, amount REAL)');

        for ($i = 1; $i <= $count; $i++) {
            $connection->insert('users', ['id' => $i, 'name' => 'name_' . $i, 'amount' => $i * 1.5]);
        }

        return $connection;
    }
}

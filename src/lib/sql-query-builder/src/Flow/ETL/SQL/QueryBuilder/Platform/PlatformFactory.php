<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Platform;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\MySQLPlatform as DbalMySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform as DbalPostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform as DbalSqlitePlatform;

/**
 * Factory for creating platform-specific query builder implementations.
 */
class PlatformFactory
{
    /**
     * Create a platform instance from a DBAL connection.
     */
    public static function fromConnection(Connection $connection): PlatformInterface
    {
        $dbalPlatform = $connection->getDatabasePlatform();
        
        return self::fromDbalPlatform($dbalPlatform);
    }

    /**
     * Create a platform instance from a DBAL platform.
     */
    public static function fromDbalPlatform(AbstractPlatform $dbalPlatform): PlatformInterface
    {
        return match (true) {
            $dbalPlatform instanceof DbalPostgreSQLPlatform => new PostgreSQLPlatform(),
            $dbalPlatform instanceof DbalMySQLPlatform => new MySQLPlatform(),
            $dbalPlatform instanceof DbalSqlitePlatform => new SQLitePlatform(),
            default => new GenericPlatform(),
        };
    }

    /**
     * Create a platform instance by name.
     */
    public static function create(string $platformName): PlatformInterface
    {
        return match (strtolower($platformName)) {
            'postgresql', 'postgres', 'pgsql' => new PostgreSQLPlatform(),
            'mysql', 'mariadb' => new MySQLPlatform(),
            'sqlite', 'sqlite3' => new SQLitePlatform(),
            default => new GenericPlatform(),
        };
    }
}
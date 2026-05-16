<?php

declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SQLitePlatform;
use Flow\Doctrine\Bulk\DbalPlatform;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use Flow\Doctrine\Bulk\Dialect\SqliteDialect;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

final class DbalPlatformTest extends TestCase
{
    public function test_is_mysql(): void
    {
        // @mago-expect analysis:deprecated-class
        $platform = new DbalPlatform(new MySQL80Platform());

        static::assertInstanceOf(MySQLDialect::class, $platform->dialect());
    }

    public function test_is_mysql_with_mariadb(): void
    {
        $platform = new DbalPlatform(new MariaDBPlatform());

        static::assertInstanceOf(MySQLDialect::class, $platform->dialect());
    }

    public function test_is_postgres_sql(): void
    {
        $platform = new DbalPlatform(new PostgreSQLPlatform());

        static::assertInstanceOf(PostgreSQLDialect::class, $platform->dialect());
    }

    public function test_is_sqlite_sql_with_lowercase_l_class_name(): void
    {
        if (!\class_exists(\Doctrine\DBAL\Platforms\SqlitePlatform::class)) {
            static::markTestSkipped(
                'Doctrine\\DBAL\\Platforms\\SqlitePlatform class is not available on this DBAL version.',
            );
        }

        $platform = new DbalPlatform(new \Doctrine\DBAL\Platforms\SqlitePlatform());

        static::assertInstanceOf(SqliteDialect::class, $platform->dialect());
    }

    public function test_is_sqlite_sql_with_uppercase_l_class_name(): void
    {
        $platform = new DbalPlatform(new SQLitePlatform());

        static::assertInstanceOf(SqliteDialect::class, $platform->dialect());
    }

    public function test_no_supported_platform(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Database platform "Doctrine\DBAL\Platforms\OraclePlatform" is not yet supported',
        );

        $platform = new DbalPlatform(new OraclePlatform());
        $platform->dialect();
    }
}

<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\PostgreSqlPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Flow\Doctrine\Bulk\DbalPlatform;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use Flow\Doctrine\Bulk\Dialect\SqliteDialect;
use PHPUnit\Framework\TestCase;

final class DbalPlatformTest extends TestCase
{
    public function test_is_mysql() : void
    {
        $platform = new DbalPlatform(new MySQL80Platform());

        $this->assertInstanceOf(MySQLDialect::class, $platform->dialect());
    }

    public function test_is_postgres_sql() : void
    {
        $platform = new DbalPlatform(new PostgreSqlPlatform());

        $this->assertInstanceOf(PostgreSQLDialect::class, $platform->dialect());
    }

    public function test_is_sqlite_sql() : void
    {
        $platform = new DbalPlatform(new SqlitePlatform());

        $this->assertInstanceOf(SqliteDialect::class, $platform->dialect());
    }
}

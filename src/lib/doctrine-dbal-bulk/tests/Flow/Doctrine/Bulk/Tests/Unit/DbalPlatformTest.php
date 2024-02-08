<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQL80Platform;
use Doctrine\DBAL\Platforms\OraclePlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Flow\Doctrine\Bulk\DbalPlatform;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use Flow\Doctrine\Bulk\Dialect\SqliteDialect;
use Flow\Doctrine\Bulk\Exception\RuntimeException;
use PHPUnit\Framework\TestCase;

final class DbalPlatformTest extends TestCase
{
    public static function provideSQLitePlatform() : iterable
    {
        yield 'legacy' => ['Doctrine\DBAL\Platforms\SqlitePlatform'];
        yield 'new' => ['Doctrine\DBAL\Platforms\SQLitePlatform'];
    }

    public function test_is_mysql() : void
    {
        $platform = new DbalPlatform(new MySQL80Platform());

        $this->assertInstanceOf(MySQLDialect::class, $platform->dialect());
    }

    public function test_is_mysql_with_mariadb() : void
    {
        $platform = new DbalPlatform(new MariaDBPlatform());

        $this->assertInstanceOf(MySQLDialect::class, $platform->dialect());
    }

    public function test_is_postgres_sql() : void
    {
        $platform = new DbalPlatform(new PostgreSQLPlatform());

        $this->assertInstanceOf(PostgreSQLDialect::class, $platform->dialect());
    }

    /**
     * @dataProvider provideSQLitePlatform
     */
    public function test_is_sqlite_sql(string $className) : void
    {
        if (\class_exists($className)) {
            $platform = new DbalPlatform(new $className());
        } else {
            $this->markTestSkipped('Unknown platform class: ' . $className);
        }

        $this->assertInstanceOf(SqliteDialect::class, $platform->dialect());
    }

    public function test_no_supported_platform() : void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Database platform "Doctrine\DBAL\Platforms\OraclePlatform" is not yet supported');

        $platform = new DbalPlatform(new OraclePlatform());
        $platform->dialect();
    }
}

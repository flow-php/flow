<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk\Tests\Unit;

use Doctrine\DBAL\Platforms\PostgreSQL94Platform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Flow\Doctrine\Bulk\QueryFactory\DbalPlatform;
use PHPUnit\Framework\TestCase;

final class DbalPlatformTest extends TestCase
{
    public function test_is_postgres_sql_for_dbal_3_2() : void
    {
        if (!\class_exists(PostgreSQLPlatform::class)) {
            $this->markTestSkipped('DBAL version < 3.2');
        }

        $platform = new DbalPlatform(new PostgreSQLPlatform());

        $this->assertTrue($platform->isPostgreSQL());
    }

    public function test_is_postgres_sql_for_dbal_less_than_3_2() : void
    {
        if (\class_exists(PostgreSQLPlatform::class)) {
            $this->markTestSkipped('DBAL version >= 3.2');
        }

        $platform = new DbalPlatform(new PostgreSQL94Platform());

        $this->assertTrue($platform->isPostgreSQL());
    }

    public function test_is_no_postgres_sql() : void
    {
        $platform = new DbalPlatform(new SqlitePlatform());

        $this->assertFalse($platform->isPostgreSQL());
    }
}

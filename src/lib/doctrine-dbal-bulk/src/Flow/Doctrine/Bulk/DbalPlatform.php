<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\MariaDBPlatform;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Flow\Doctrine\Bulk\Dialect\Dialect;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use Flow\Doctrine\Bulk\Dialect\SqliteDialect;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

/**
 * @infection-ignore-all
 */
final class DbalPlatform
{
    public function __construct(private readonly AbstractPlatform $platform)
    {
    }

    public function dialect() : Dialect
    {
        if ($this->isPostgreSQL()) {
            return new PostgreSQLDialect($this->platform);
        }

        if ($this->isMySQL() || $this->isMariaDB()) {
            return new MySQLDialect($this->platform);
        }

        if ($this->isSqlite()) {
            return new SqliteDialect($this->platform);
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not yet supported',
            \get_class($this->platform)
        ));
    }

    private function isMariaDB() : bool
    {
        return $this->platform instanceof MariaDBPlatform;
    }

    private function isMySQL() : bool
    {
        return $this->platform instanceof MySQLPlatform;
    }

    private function isPostgreSQL() : bool
    {
        return $this->platform instanceof PostgreSQLPlatform;
    }

    private function isSqlite() : bool
    {
        return \in_array($this->platform::class, ['Doctrine\DBAL\Platforms\SqlitePlatform', 'Doctrine\DBAL\Platforms\SQLitePlatform'], true);
    }
}

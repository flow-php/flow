<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Doctrine\DBAL\Platforms\SqlitePlatform;
use Flow\Doctrine\Bulk\Dialect\Dialect;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use Flow\Doctrine\Bulk\Dialect\SqliteDialect;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

/**
 * @infection-ignore-all
 *
 * @codeCoverageIgnore
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

        if ($this->isMySQL()) {
            return new MySQLDialect();
        }

        if ($this->isSqlite()) {
            return new SqliteDialect();
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not yet supported',
            \get_class($this->platform)
        ));
    }

    private function isMySQL() : bool
    {
        if (\class_exists(MySQLPlatform::class)) {
            return $this->platform instanceof MySQLPlatform;
        }

        /**
         * @psalm-suppress DeprecatedMethod
         */
        return $this->platform->getName() === 'mysql';
    }

    private function isPostgreSQL() : bool
    {
        if (\class_exists(PostgreSQLPlatform::class)) {
            return $this->platform instanceof PostgreSQLPlatform;
        }

        /**
         * @psalm-suppress DeprecatedMethod
         */
        return $this->platform->getName() === 'postgresql';
    }

    private function isSqlite() : bool
    {
        return $this->platform instanceof SqlitePlatform;
    }
}

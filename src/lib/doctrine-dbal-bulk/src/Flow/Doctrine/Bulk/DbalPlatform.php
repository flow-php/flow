<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\MySQLPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Flow\Doctrine\Bulk\Dialect\Dialect;
use Flow\Doctrine\Bulk\Dialect\MySQLDialect;
use Flow\Doctrine\Bulk\Dialect\PostgreSQLDialect;
use Flow\Doctrine\Bulk\Exception\RuntimeException;

final class DbalPlatform
{
    private AbstractPlatform $platform;

    public function __construct(AbstractPlatform $platform)
    {
        $this->platform = $platform;
    }

    public function dialect() : Dialect
    {
        if ($this->isPostgreSQL()) {
            return new PostgreSQLDialect($this->platform);
        }

        if ($this->isMysql()) {
            return new MySQLDialect();
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not yet supported',
            \get_class($this->platform)
        ));
    }

    private function isMysql() : bool
    {
        return $this->platform instanceof MySQLPlatform;
    }

    private function isPostgreSQL() : bool
    {
        return $this->platform instanceof PostgreSQLPlatform;
    }
}

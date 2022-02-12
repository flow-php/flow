<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;
use Flow\Doctrine\Bulk\Dialect\Dialect;
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
            return new PostgreSQLDialect();
        }

        throw new RuntimeException(\sprintf(
            'Database platform "%s" is not yet supported',
            \get_class($this->platform)
        ));
    }

    private function isPostgreSQL() : bool
    {
        // DBAL version >= 3.2
        if (\class_exists(PostgreSQLPlatform::class)) {
            return $this->platform instanceof PostgreSQLPlatform;
        }

        /**
         * @psalm-suppress DeprecatedMethod
         */
        return $this->platform->getName() === 'postgresql';
    }
}

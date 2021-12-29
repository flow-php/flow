<?php declare(strict_types=1);

namespace Flow\Doctrine\Bulk\QueryFactory;

use Doctrine\DBAL\Platforms\AbstractPlatform;
use Doctrine\DBAL\Platforms\PostgreSQLPlatform;

final class DbalPlatform
{
    private AbstractPlatform $platform;

    public function __construct(AbstractPlatform $platform)
    {
        $this->platform = $platform;
    }

    public function isPostgreSQL() : bool
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

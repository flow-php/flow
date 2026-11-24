<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Exception;

use function sprintf;

final class MigrationLockedException extends MigrationException
{
    public function __construct(string $lockName)
    {
        parent::__construct(sprintf(
            'Another process is running migrations (lock "%s"). Run the command again when it finishes.',
            $lockName,
        ));
    }
}

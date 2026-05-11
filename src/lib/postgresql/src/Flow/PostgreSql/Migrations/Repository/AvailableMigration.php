<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Repository;

use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\Rollback;
use Flow\PostgreSql\Migrations\Version;

final readonly class AvailableMigration
{
    public function __construct(
        public Version $version,
        public string $name,
        public Migration $migration,
        public ?Rollback $rollback,
    ) {}
}

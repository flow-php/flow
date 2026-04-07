<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

final readonly class MigrationPlan
{
    public function __construct(
        public Version $version,
        public Migration $migration,
        public ?Rollback $rollback,
        public Direction $direction,
    ) {
    }
}

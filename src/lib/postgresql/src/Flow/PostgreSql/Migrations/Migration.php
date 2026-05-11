<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

interface Migration
{
    public function migrate(MigrationContext $context): void;

    public function transactional(): bool;
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

interface Rollback
{
    public function rollback(MigrationContext $context) : void;

    public function transactional() : bool;
}

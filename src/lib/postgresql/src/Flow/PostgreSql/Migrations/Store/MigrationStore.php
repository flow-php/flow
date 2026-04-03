<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Store;

use Flow\PostgreSql\Migrations\Version;

interface MigrationStore
{
    public function complete(Version $version, int $executionTimeMs) : void;

    public function executedMigrations() : ExecutedMigrations;

    public function initialize() : void;

    public function isInitialized() : bool;

    public function remove(Version $version) : void;

    public function reset() : void;
}

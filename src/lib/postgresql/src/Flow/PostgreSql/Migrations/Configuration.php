<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Schema\CatalogProvider;
use Flow\PostgreSql\Schema\Exclusion\ExclusionPolicy;

final readonly class Configuration
{
    public function __construct(
        public Client $client,
        public CatalogProvider $targetCatalogProvider,
        public string $migrationsDirectory,
        public string $migrationsNamespace,
        public string $tableName = 'flow_migrations',
        public string $tableSchema = 'public',
        public string $migrationFileName = 'migration.php',
        public string $rollbackFileName = 'rollback.php',
        public bool $allOrNothing = false,
        public bool $generateRollback = true,
        public ?ExclusionPolicy $exclusionPolicy = null,
    ) {}
}

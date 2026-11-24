<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Context;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Lock\MigrationLock;
use Flow\PostgreSql\Migrations\MigrationMode;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationExecutor;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationLock;
use Flow\PostgreSql\Schema\Catalog;

final class MigratorContext
{
    public SpyClient $client;

    public SpyMigrationExecutor $executor;

    public SpyMigrationLock $lock;

    public FakeMigrationStore $store;

    public function __construct()
    {
        $this->client = new SpyClient();
        $this->executor = new SpyMigrationExecutor();
        $this->lock = new SpyMigrationLock();
        $this->store = new FakeMigrationStore();
    }

    /**
     * @param list<AvailableMigration> $migrations
     * @param array<string, mixed> $attributes
     */
    public function migrator(
        array $migrations = [],
        MigrationMode $mode = MigrationMode::ALL_PENDING,
        bool $allOrNothing = false,
        array $attributes = [],
        ?MigrationLock $lock = null,
    ): Migrator {
        return new Migrator(
            new FakeMigrationRepository(...$migrations),
            $this->store,
            $this->executor,
            $this->client,
            new Configuration(
                $this->client,
                new FakeCatalogProvider(new Catalog([])),
                '/tmp',
                'App\\Migrations',
                allOrNothing: $allOrNothing,
                attributes: $attributes,
                mode: $mode,
                lock: $lock,
            ),
        );
    }
}

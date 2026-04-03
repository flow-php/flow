<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use function Flow\PostgreSql\DSL\{catalog_comparator, client_catalog_provider};

use Flow\PostgreSql\Migrations\Executor\{DefaultMigrationExecutor, MigrationExecutor};
use Flow\PostgreSql\Migrations\Generator\{DiffMigrationGenerator, MigrationGenerator};
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\{MigrationStore, PostgreSqlMigrationStore};

final readonly class MigrationsFactory
{
    public function __construct(
        private Configuration $configuration,
        private MigrationRepository $repository,
    ) {
    }

    public function createDiffGenerator(MigrationGenerator $generator) : DiffMigrationGenerator
    {
        return new DiffMigrationGenerator(
            client_catalog_provider($this->configuration->client, excludeTables: [$this->configuration->tableName]),
            $this->configuration->targetCatalogProvider,
            catalog_comparator(),
            $generator,
            $this->configuration->generateRollback,
        );
    }

    public function createExecutor() : MigrationExecutor
    {
        return new DefaultMigrationExecutor();
    }

    public function createMigrator() : Migrator
    {
        return new Migrator(
            $this->repository,
            $this->createStore(),
            $this->createExecutor(),
            $this->configuration->client,
            $this->configuration,
        );
    }

    public function createStore() : MigrationStore
    {
        return new PostgreSqlMigrationStore($this->configuration->client, $this->configuration);
    }

    public function createVersionResolver() : VersionResolver
    {
        return new VersionResolver(
            $this->repository,
            $this->createStore(),
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations;

use Flow\PostgreSql\Migrations\Executor\DefaultMigrationExecutor;
use Flow\PostgreSql\Migrations\Executor\MigrationExecutor;
use Flow\PostgreSql\Migrations\Generator\DiffMigrationGenerator;
use Flow\PostgreSql\Migrations\Generator\MigrationGenerator;
use Flow\PostgreSql\Migrations\Repository\MigrationRepository;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\Store\PostgreSqlMigrationStore;
use Flow\PostgreSql\Schema\Exclusion\AnyExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\ExactMatchExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\SchemaObjectType;
use Flow\PostgreSql\Schema\Exclusion\ScopedExclusionPolicy;

use function Flow\PostgreSql\DSL\catalog_comparator;
use function Flow\PostgreSql\DSL\client_catalog_provider;

final readonly class MigrationsFactory
{
    public function __construct(
        private Configuration $configuration,
        private MigrationRepository $repository,
    ) {}

    public function createDiffGenerator(MigrationGenerator $generator): DiffMigrationGenerator
    {
        $migrationsTablePolicy = new ScopedExclusionPolicy(
            new ExactMatchExclusionPolicy($this->configuration->tableName),
            SchemaObjectType::TABLE,
            $this->configuration->tableSchema,
        );

        $exclusionPolicy = $this->configuration->exclusionPolicy === null
            ? $migrationsTablePolicy
            : new AnyExclusionPolicy($migrationsTablePolicy, $this->configuration->exclusionPolicy);

        return new DiffMigrationGenerator(
            client_catalog_provider($this->configuration->client, exclusionPolicy: $exclusionPolicy),
            $this->configuration->targetCatalogProvider,
            catalog_comparator(dropIfExists: $this->configuration->dropIfExists),
            $generator,
            $this->configuration->generateRollback,
        );
    }

    public function createExecutor(): MigrationExecutor
    {
        return new DefaultMigrationExecutor();
    }

    public function createMigrator(): Migrator
    {
        return new Migrator(
            $this->repository,
            $this->createStore(),
            $this->createExecutor(),
            $this->configuration->client,
            $this->configuration,
        );
    }

    public function createStore(): MigrationStore
    {
        return new PostgreSqlMigrationStore($this->configuration->client, $this->configuration);
    }

    public function createVersionResolver(): VersionResolver
    {
        return new VersionResolver($this->repository, $this->createStore());
    }
}

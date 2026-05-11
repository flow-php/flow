<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Generator;

use Flow\PostgreSql\Migrations\Exception\MigrationException;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;
use Flow\PostgreSql\Schema\Diff\CatalogComparator;

final readonly class DiffMigrationGenerator
{
    public function __construct(
        private CatalogProvider $sourceCatalog,
        private CatalogProvider $targetCatalog,
        private CatalogComparator $comparator,
        private MigrationGenerator $generator,
        private bool $generateRollback = true,
    ) {}

    public function generate(?string $name = null, bool $allowEmpty = false, bool $fromEmptySchema = false): Version
    {
        $source = $fromEmptySchema ? new Catalog([]) : $this->sourceCatalog->get();
        $target = $this->targetCatalog->get();

        $diff = $this->comparator->compare($source, $target);

        if ($diff->isEmpty() && !$allowEmpty) {
            throw MigrationException::noChangesDetected();
        }

        $upSql = \array_map(static fn(Sql $query) => $query->toSql(), $diff->generate());

        $downSql = null;

        if ($this->generateRollback) {
            $downSql = \array_map(
                static fn(Sql $query) => $query->toSql(),
                $this->comparator->compare($target, $this->sourceCatalog->get())->generate(),
            );
        }

        return $this->generator->generateSchemaMigration($name, $upSql, $downSql);
    }
}

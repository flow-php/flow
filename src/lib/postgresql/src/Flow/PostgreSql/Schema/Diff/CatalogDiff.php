<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\ExecutionOrderStrategy;
use Flow\PostgreSql\Schema\Extension;
use Flow\PostgreSql\Schema\ForeignKeyDependencyOrder;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\MaterializedViewDependencyOrder;
use Flow\PostgreSql\Schema\Schema;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\View;
use Flow\PostgreSql\Schema\ViewDependencyOrder;

use function array_filter;
use function array_flip;
use function array_key_exists;
use function array_map;
use function array_values;
use function Flow\PostgreSql\DSL\create;
use function Flow\PostgreSql\DSL\drop;
use function in_array;

final readonly class CatalogDiff implements Diff
{
    /**
     * @param list<Schema> $addedSchemas
     * @param list<Schema> $removedSchemas
     * @param list<SchemaDiff> $modifiedSchemas
     * @param list<Extension> $addedExtensions
     * @param list<Extension> $removedExtensions
     * @param list<ExtensionDiff> $modifiedExtensions
     * @param ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     */
    public function __construct(
        public Catalog $source,
        public Catalog $target,
        public array $addedSchemas = [],
        public array $removedSchemas = [],
        public array $modifiedSchemas = [],
        public array $addedExtensions = [],
        public array $removedExtensions = [],
        public array $modifiedExtensions = [],
        private ViewDependencyResolver $viewDependencyResolver = new NoopViewDependencyResolver(),
        private ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        private ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        private ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(
            new Parser(),
        ),
        private bool $dropIfExists = false,
    ) {}

    /**
     * @return list<Sql>
     */
    public function generate(): array
    {
        $sqls = [];

        foreach ($this->addedSchemas as $schema) {
            $sqls[] = create()->schema($schema->name)->ifNotExists();
        }

        foreach ($this->addedExtensions as $extension) {
            $sqls[] = $extension->toSql();
        }

        foreach ($this->modifiedExtensions as $extensionDiff) {
            $sqls = [...$sqls, ...$extensionDiff->generate()];
        }

        foreach ($this->addedSchemas as $schema) {
            $sqls = [
                ...$sqls,
                ...$schema->toSql(
                    $this->tableOrderStrategy,
                    $this->viewOrderStrategy,
                    $this->materializedViewOrderStrategy,
                ),
            ];
        }

        $dependentViews = $this->resolveDependentViews();

        foreach ($dependentViews->toDrop as $view) {
            $builder = $view instanceof MaterializedView
                ? drop()->materializedView($view->qualifiedName())
                : drop()->view($view->qualifiedName());
            $sqls[] = $this->dropIfExists ? $builder->ifExists() : $builder;
        }

        foreach ($this->modifiedSchemas as $diff) {
            $sqls = [...$sqls, ...$diff->generate()];
        }

        foreach ($dependentViews->toCreate as $view) {
            $sqls = $view instanceof MaterializedView ? [...$sqls, ...$view->toSql()] : [...$sqls, $view->toSql()];
        }

        foreach ($this->removedSchemas as $schema) {
            $builder = drop()->schema($schema->name)->cascade();
            $sqls[] = $this->dropIfExists ? $builder->ifExists() : $builder;
        }

        $removedSchemaNames = array_map(static fn(Schema $schema): string => $schema->name, $this->removedSchemas);

        foreach ($this->removedExtensions as $extension) {
            if (in_array($extension->schema, $removedSchemaNames, true)) {
                continue;
            }

            $builder = drop()->extension($extension->name);
            $sqls[] = $this->dropIfExists ? $builder->ifExists() : $builder;
        }

        return $sqls;
    }

    public function isEmpty(): bool
    {
        return (
            $this->addedSchemas === []
            && $this->removedSchemas === []
            && $this->modifiedSchemas === []
            && $this->addedExtensions === []
            && $this->removedExtensions === []
            && $this->modifiedExtensions === []
        );
    }

    /**
     * @return list<string>
     */
    private function collectModifiedTableNames(): array
    {
        $names = [];

        foreach ($this->modifiedSchemas as $schemaDiff) {
            foreach ($schemaDiff->modifiedTables as $tableDiff) {
                if ($tableDiff->requiresViewRebuild()) {
                    $names[] = $tableDiff->target->qualifiedName();
                }
            }
        }

        return $names;
    }

    private function resolveDependentViews(): DependentViews
    {
        $modifiedTableNames = $this->collectModifiedTableNames();

        if ($modifiedTableNames === []) {
            return DependentViews::empty();
        }

        $resolved = $this->viewDependencyResolver->resolve($this->source, $modifiedTableNames);

        $excludedNames = $this->viewNamesAlreadyHandledByDiff();

        if ($excludedNames === []) {
            return $resolved;
        }

        $excludeSet = array_flip($excludedNames);

        return new DependentViews(
            array_values(array_filter(
                $resolved->toDrop,
                static fn(MaterializedView|View $view): bool => !array_key_exists($view->qualifiedName(), $excludeSet),
            )),
            array_values(array_filter(
                $resolved->toCreate,
                static fn(MaterializedView|View $view): bool => !array_key_exists($view->qualifiedName(), $excludeSet),
            )),
        );
    }

    /**
     * @return list<string>
     */
    private function viewNamesAlreadyHandledByDiff(): array
    {
        $names = [];

        foreach ($this->modifiedSchemas as $schemaDiff) {
            foreach ($schemaDiff->removedViews as $view) {
                $names[] = $view->qualifiedName();
            }

            foreach ($schemaDiff->modifiedViews as $viewDiff) {
                $names[] = $viewDiff->target->qualifiedName();
            }

            foreach ($schemaDiff->removedMaterializedViews as $mv) {
                $names[] = $mv->qualifiedName();
            }

            foreach ($schemaDiff->modifiedMaterializedViews as $mvDiff) {
                $names[] = $mvDiff->target->qualifiedName();
            }
        }

        return $names;
    }
}

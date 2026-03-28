<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use function Flow\PostgreSql\DSL\{create, drop, parsed_select};
use Flow\PostgreSql\Parser;

use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexMethod as QbIndexMethod;
use Flow\PostgreSql\QueryBuilder\SqlQuery;
use Flow\PostgreSql\Schema\{Catalog, ExecutionOrderStrategy, ForeignKeyDependencyOrder, IndexMethod, MaterializedView, MaterializedViewDependencyOrder, Schema, View, ViewDependencyOrder};
use Flow\PostgreSql\Schema\Table;

final readonly class CatalogDiff implements Diff
{
    /**
     * @param list<Schema> $addedSchemas
     * @param list<Schema> $removedSchemas
     * @param list<SchemaDiff> $modifiedSchemas
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
        private ViewDependencyResolver $viewDependencyResolver = new NoopViewDependencyResolver(),
        private ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        private ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        private ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(new Parser()),
    ) {
    }

    /**
     * @return list<SqlQuery>
     */
    public function generate() : array
    {
        $sqls = [];

        foreach ($this->addedSchemas as $schema) {
            $sqls[] = create()->schema($schema->name);
            $sqls = [...$sqls, ...$schema->toSql($this->tableOrderStrategy, $this->viewOrderStrategy, $this->materializedViewOrderStrategy)];
        }

        $dependentViews = $this->resolveDependentViews();

        foreach ($dependentViews->toDrop as $dv) {
            $sqls[] = $dv->view instanceof MaterializedView
                ? drop()->materializedView($dv->qualifiedName())
                : drop()->view($dv->qualifiedName());
        }

        foreach ($this->modifiedSchemas as $diff) {
            $sqls = [...$sqls, ...$diff->generate()];
        }

        foreach ($dependentViews->toCreate as $dv) {
            if ($dv->view instanceof MaterializedView) {
                $sqls[] = create()->materializedView($dv->view->name, $dv->schema)->as(parsed_select($dv->view->definition));

                foreach ($dv->view->indexes as $idx) {
                    $builder = create()->index($idx->name);

                    if ($idx->unique) {
                        $builder = $builder->unique();
                    }

                    $onBuilder = $builder->on($dv->view->name, $dv->schema);

                    if ($idx->method !== IndexMethod::BTREE) {
                        $onBuilder = $onBuilder->using(QbIndexMethod::from($idx->method->value));
                    }

                    $sqls[] = $onBuilder->columns(...$idx->columns);
                }
            } else {
                $sqls[] = create()->view($dv->view->name, $dv->schema)->as(parsed_select($dv->view->definition));
            }
        }

        foreach ($this->removedSchemas as $schema) {
            $sqls[] = drop()->schema($schema->name)->cascade();
        }

        return $sqls;
    }

    public function isEmpty() : bool
    {
        return $this->addedSchemas === []
            && $this->removedSchemas === []
            && $this->modifiedSchemas === [];
    }

    /**
     * @return list<string>
     */
    private function collectModifiedTableNames() : array
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

    private function resolveDependentViews() : DependentViews
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

        $excludeSet = \array_flip($excludedNames);

        return new DependentViews(
            \array_values(\array_filter($resolved->toDrop, static fn (DependentView $dv) : bool => !\array_key_exists($dv->qualifiedName(), $excludeSet))),
            \array_values(\array_filter($resolved->toCreate, static fn (DependentView $dv) : bool => !\array_key_exists($dv->qualifiedName(), $excludeSet))),
        );
    }

    /**
     * @return list<string>
     */
    private function viewNamesAlreadyHandledByDiff() : array
    {
        $names = [];

        foreach ($this->modifiedSchemas as $schemaDiff) {
            foreach ($schemaDiff->removedViews as $view) {
                $names[] = $schemaDiff->source->name . '.' . $view->name;
            }

            foreach ($schemaDiff->modifiedViews as $viewDiff) {
                $names[] = $schemaDiff->source->name . '.' . $viewDiff->target->name;
            }

            foreach ($schemaDiff->removedMaterializedViews as $mv) {
                $names[] = $schemaDiff->source->name . '.' . $mv->name;
            }

            foreach ($schemaDiff->modifiedMaterializedViews as $mvDiff) {
                $names[] = $schemaDiff->source->name . '.' . $mvDiff->target->name;
            }
        }

        return $names;
    }
}

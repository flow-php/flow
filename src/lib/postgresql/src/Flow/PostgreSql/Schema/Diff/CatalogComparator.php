<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\ExecutionOrderStrategy;
use Flow\PostgreSql\Schema\ForeignKeyDependencyOrder;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\MaterializedViewDependencyOrder;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\View;
use Flow\PostgreSql\Schema\ViewDependencyOrder;

use function in_array;

final readonly class CatalogComparator
{
    /**
     * @param ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     */
    public function __construct(
        private SchemaComparator $schemaComparator,
        private ViewDependencyResolver $viewDependencyResolver = new AstViewDependencyResolver(new Parser()),
        private ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        private ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        private ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(
            new Parser(),
        ),
        private bool $dropIfExists = false,
    ) {}

    /**
     * @param null|ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param null|ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param null|ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     */
    public static function create(
        ?RenameStrategy $renameStrategy = null,
        ?ViewDependencyResolver $viewDependencyResolver = null,
        ?ExecutionOrderStrategy $tableOrderStrategy = null,
        ?ExecutionOrderStrategy $viewOrderStrategy = null,
        ?ExecutionOrderStrategy $materializedViewOrderStrategy = null,
        bool $dropIfExists = false,
    ): self {
        $renameStrategy ??= new GreedySimilarityRenameStrategy(new SimilarTextStrategy());
        $constraintComparator = new ConstraintComparator();
        $indexComparator = new IndexComparator($renameStrategy);
        $tableComparator = new TableComparator($indexComparator, $constraintComparator, $renameStrategy);
        $tableStructureComparator = new TableStructureComparator($renameStrategy);

        $tableOrderStrategy ??= new ForeignKeyDependencyOrder();
        $viewOrderStrategy ??= new ViewDependencyOrder(new Parser());
        $materializedViewOrderStrategy ??= new MaterializedViewDependencyOrder(new Parser());

        return new self(
            new SchemaComparator(
                $tableComparator,
                $indexComparator,
                $constraintComparator,
                $tableStructureComparator,
                $tableOrderStrategy,
                $viewOrderStrategy,
                $materializedViewOrderStrategy,
                dropIfExists: $dropIfExists,
            ),
            $viewDependencyResolver ?? new AstViewDependencyResolver(new Parser()),
            $tableOrderStrategy,
            $viewOrderStrategy,
            $materializedViewOrderStrategy,
            dropIfExists: $dropIfExists,
        );
    }

    public function compare(Catalog $source, Catalog $target, ?bool $dropIfExists = null): CatalogDiff
    {
        $effectiveDropIfExists = $dropIfExists ?? $this->dropIfExists;
        $sourceNames = $source->names();
        $targetNames = $target->names();

        $addedSchemas = [];
        $removedSchemas = [];
        $modifiedSchemas = [];

        foreach ($targetNames as $name) {
            if (!in_array($name, $sourceNames, true)) {
                $addedSchemas[] = $target->get($name);
            }
        }

        foreach ($sourceNames as $name) {
            if (!in_array($name, $targetNames, true)) {
                $removedSchemas[] = $source->get($name);
            }
        }

        foreach ($sourceNames as $name) {
            if (!in_array($name, $targetNames, true)) {
                continue;
            }

            $schemaDiff = $this->schemaComparator->compare(
                $source->get($name),
                $target->get($name),
                $effectiveDropIfExists,
            );

            if (!$schemaDiff->isEmpty()) {
                $modifiedSchemas[] = $schemaDiff;
            }
        }

        return new CatalogDiff(
            $source,
            $target,
            $addedSchemas,
            $removedSchemas,
            $modifiedSchemas,
            $this->viewDependencyResolver,
            $this->tableOrderStrategy,
            $this->viewOrderStrategy,
            $this->materializedViewOrderStrategy,
            $effectiveDropIfExists,
        );
    }
}

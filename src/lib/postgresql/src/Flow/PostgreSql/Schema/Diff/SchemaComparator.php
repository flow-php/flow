<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\Schema\Domain;
use Flow\PostgreSql\Schema\ExecutionOrderStrategy;
use Flow\PostgreSql\Schema\Extension;
use Flow\PostgreSql\Schema\ForeignKeyDependencyOrder;
use Flow\PostgreSql\Schema\Func;
use Flow\PostgreSql\Schema\MaterializedView;
use Flow\PostgreSql\Schema\MaterializedViewDependencyOrder;
use Flow\PostgreSql\Schema\Procedure;
use Flow\PostgreSql\Schema\Schema;
use Flow\PostgreSql\Schema\Sequence;
use Flow\PostgreSql\Schema\Table;
use Flow\PostgreSql\Schema\View;
use Flow\PostgreSql\Schema\ViewDependencyOrder;
use Throwable;

final readonly class SchemaComparator
{
    /**
     * @param ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     */
    public function __construct(
        private TableComparator $tableComparator,
        private IndexComparator $indexComparator,
        private ConstraintComparator $constraintComparator,
        private TableStructureComparator $tableStructureComparator,
        private ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        private ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        private ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(
            new Parser(),
        ),
        private Parser $parser = new Parser(),
    ) {}

    public function compare(Schema $source, Schema $target): SchemaDiff
    {
        $tables = $this->diffTables($source->tables, $target->tables);
        $materializedViews = $this->diffMaterializedViews($source->materializedViews, $target->materializedViews);
        $domains = $this->diffDomains($source->domains, $target->domains);

        $sequences = ChangeSet::fromNamedObjects(
            $source->sequences,
            $target->sequences,
            static fn(Sequence $s): string => $s->name,
            static fn(Sequence $a, Sequence $b): ?SequenceDiff => $a->dataType === $b->dataType
                && $a->startValue === $b->startValue
                && $a->minValue === $b->minValue
                && $a->maxValue === $b->maxValue
                && $a->incrementBy === $b->incrementBy
                && $a->cycle === $b->cycle
                && $a->cacheValue === $b->cacheValue
                && $a->ownedByTable === $b->ownedByTable
                && $a->ownedByColumn === $b->ownedByColumn
                    ? null
                    : new SequenceDiff($a, $b),
        );

        $views = ChangeSet::fromNamedObjects(
            $source->views,
            $target->views,
            static fn(View $v): string => $v->name,
            fn(View $a, View $b): ?ViewDiff => $this->definitionsEqual($a->definition, $b->definition)
                && $a->isUpdatable === $b->isUpdatable
                    ? null
                    : new ViewDiff($a, $b),
        );

        $functions = ChangeSet::fromNamedObjects(
            $source->functions,
            $target->functions,
            static fn(Func $f): string => $f->name,
            static fn(Func $a, Func $b): ?FuncDiff => $a->returnType === $b->returnType
                && $a->argumentTypes === $b->argumentTypes
                && $a->language === $b->language
                && $a->definition === $b->definition
                && $a->isStrict === $b->isStrict
                && $a->volatility === $b->volatility
                    ? null
                    : new FuncDiff($a, $b),
        );

        $procedures = ChangeSet::fromNamedObjects(
            $source->procedures,
            $target->procedures,
            static fn(Procedure $p): string => $p->name,
            static fn(Procedure $a, Procedure $b): ?ProcedureDiff => $a->argumentTypes === $b->argumentTypes
                && $a->language === $b->language
                && $a->definition === $b->definition
                    ? null
                    : new ProcedureDiff($a, $b),
        );

        $extensions = ChangeSet::fromNamedObjects(
            $source->extensions,
            $target->extensions,
            static fn(Extension $e): string => $e->name,
            static fn(Extension $a, Extension $b): ?ExtensionDiff => $a->version === $b->version
                ? null
                : new ExtensionDiff($a, $b),
        );

        return new SchemaDiff(
            $source,
            $target,
            $tables->added,
            $tables->removed,
            $tables->modified ?? [],
            $tables->renamed ?? [],
            $sequences->added,
            $sequences->removed,
            $sequences->modified ?? [],
            $views->added,
            $views->removed,
            $views->modified ?? [],
            $materializedViews->added,
            $materializedViews->removed,
            $materializedViews->modified ?? [],
            $functions->added,
            $functions->removed,
            $functions->modified ?? [],
            $procedures->added,
            $procedures->removed,
            $procedures->modified ?? [],
            $domains->added,
            $domains->removed,
            $domains->modified ?? [],
            $extensions->added,
            $extensions->removed,
            $extensions->modified ?? [],
            $this->tableOrderStrategy,
            $this->viewOrderStrategy,
            $this->materializedViewOrderStrategy,
        );
    }

    private function definitionsEqual(string $a, string $b): bool
    {
        return $this->normalizeDefinition($a) === $this->normalizeDefinition($b);
    }

    /**
     * @param list<Domain> $sourceDomains
     * @param list<Domain> $targetDomains
     *
     * @return ChangeSet<Domain, DomainDiff>
     */
    private function diffDomains(array $sourceDomains, array $targetDomains): ChangeSet
    {
        return ChangeSet::fromNamedObjects(
            $sourceDomains,
            $targetDomains,
            static fn(Domain $d): string => $d->name,
            function (Domain $a, Domain $b): ?DomainDiff {
                $checkDiff = $this->constraintComparator->diffCheckConstraints(
                    $a->checkConstraints,
                    $b->checkConstraints,
                );

                $hasPropertyChange =
                    !$a->baseType->isEqual($b->baseType)
                    || $a->nullable !== $b->nullable
                    || $a->default !== $b->default;

                if (!$hasPropertyChange && $checkDiff->added === [] && $checkDiff->removed === []) {
                    return null;
                }

                return new DomainDiff($a, $b, $checkDiff->added, $checkDiff->removed);
            },
        );
    }

    /**
     * @param list<MaterializedView> $sourceMViews
     * @param list<MaterializedView> $targetMViews
     *
     * @return ChangeSet<MaterializedView, MaterializedViewDiff>
     */
    private function diffMaterializedViews(array $sourceMViews, array $targetMViews): ChangeSet
    {
        return ChangeSet::fromNamedObjects(
            $sourceMViews,
            $targetMViews,
            static fn(MaterializedView $mv): string => $mv->name,
            function (MaterializedView $a, MaterializedView $b): ?MaterializedViewDiff {
                $indexChanges = $this->indexComparator->compare($a->indexes, $b->indexes);

                if (
                    $this->definitionsEqual($a->definition, $b->definition)
                    && $indexChanges->added === []
                    && $indexChanges->removed === []
                    && ($indexChanges->renamed === null || $indexChanges->renamed === [])
                ) {
                    return null;
                }

                return new MaterializedViewDiff($a, $b, $indexChanges->added, $indexChanges->removed);
            },
        );
    }

    /**
     * @param list<Table> $sourceTables
     * @param list<Table> $targetTables
     *
     * @return ChangeSet<Table, TableDiff>
     */
    private function diffTables(array $sourceTables, array $targetTables): ChangeSet
    {
        $initial = ChangeSet::fromNamedObjects(
            $sourceTables,
            $targetTables,
            static fn(Table $t): string => $t->name,
            fn(Table $a, Table $b): ?TableDiff => ($diff = $this->tableComparator->compare($a, $b))->isEmpty()
                ? null
                : $diff,
        );

        $renameResult = $this->tableStructureComparator->detectTableRenames($initial->added, $initial->removed);

        return new ChangeSet($renameResult->added, $renameResult->removed, $initial->modified, $renameResult->renamed);
    }

    private function normalizeDefinition(string $definition): string
    {
        try {
            return $this->parser->parse($definition)->deparse();
        } catch (Throwable) {
            return $definition;
        }
    }
}

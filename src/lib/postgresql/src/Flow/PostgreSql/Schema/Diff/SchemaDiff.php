<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema\Diff;

use function Flow\PostgreSql\DSL\{alter, drop};

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\Sql;
use Flow\PostgreSql\Schema\{Domain, ExecutionOrderStrategy, Extension, ForeignKeyDependencyOrder, Func, MaterializedView, MaterializedViewDependencyOrder, Procedure, Schema, Sequence, Table, View, ViewDependencyOrder};

final readonly class SchemaDiff implements Diff
{
    /**
     * @param list<Table> $addedTables
     * @param list<Table> $removedTables
     * @param list<TableDiff> $modifiedTables
     * @param array<string, Table> $renamedTables old qualified name => new Table
     * @param list<Sequence> $addedSequences
     * @param list<Sequence> $removedSequences
     * @param list<SequenceDiff> $modifiedSequences
     * @param list<View> $addedViews
     * @param list<View> $removedViews
     * @param list<ViewDiff> $modifiedViews
     * @param list<MaterializedView> $addedMaterializedViews
     * @param list<MaterializedView> $removedMaterializedViews
     * @param list<MaterializedViewDiff> $modifiedMaterializedViews
     * @param list<Func> $addedFunctions
     * @param list<Func> $removedFunctions
     * @param list<FuncDiff> $modifiedFunctions
     * @param list<Procedure> $addedProcedures
     * @param list<Procedure> $removedProcedures
     * @param list<ProcedureDiff> $modifiedProcedures
     * @param list<Domain> $addedDomains
     * @param list<Domain> $removedDomains
     * @param list<DomainDiff> $modifiedDomains
     * @param list<Extension> $addedExtensions
     * @param list<Extension> $removedExtensions
     * @param list<ExtensionDiff> $modifiedExtensions
     * @param ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     */
    public function __construct(
        public Schema $source,
        public Schema $target,
        public array $addedTables = [],
        public array $removedTables = [],
        public array $modifiedTables = [],
        public array $renamedTables = [],
        public array $addedSequences = [],
        public array $removedSequences = [],
        public array $modifiedSequences = [],
        public array $addedViews = [],
        public array $removedViews = [],
        public array $modifiedViews = [],
        public array $addedMaterializedViews = [],
        public array $removedMaterializedViews = [],
        public array $modifiedMaterializedViews = [],
        public array $addedFunctions = [],
        public array $removedFunctions = [],
        public array $modifiedFunctions = [],
        public array $addedProcedures = [],
        public array $removedProcedures = [],
        public array $modifiedProcedures = [],
        public array $addedDomains = [],
        public array $removedDomains = [],
        public array $modifiedDomains = [],
        public array $addedExtensions = [],
        public array $removedExtensions = [],
        public array $modifiedExtensions = [],
        private ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        private ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        private ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(new Parser()),
    ) {
    }

    /**
     * @return list<Sql>
     */
    public function generate() : array
    {
        return [
            ...(new Schema($this->target->name, $this->addedTables, $this->addedSequences, $this->addedViews, $this->addedMaterializedViews, $this->addedFunctions, $this->addedProcedures, $this->addedDomains, $this->addedExtensions))->toSql($this->tableOrderStrategy, $this->viewOrderStrategy, $this->materializedViewOrderStrategy),
            ...$this->modifiedSqls(),
            ...$this->renamedTableSqls(),
            ...$this->dropObjectSqls($this->removedMaterializedViews, $this->removedViews, $this->removedTables, $this->removedProcedures, $this->removedFunctions, $this->removedSequences, $this->removedDomains, $this->removedExtensions),
        ];
    }

    public function isEmpty() : bool
    {
        return $this->addedTables === []
            && $this->removedTables === []
            && $this->modifiedTables === []
            && $this->renamedTables === []
            && $this->addedSequences === []
            && $this->removedSequences === []
            && $this->modifiedSequences === []
            && $this->addedViews === []
            && $this->removedViews === []
            && $this->modifiedViews === []
            && $this->addedMaterializedViews === []
            && $this->removedMaterializedViews === []
            && $this->modifiedMaterializedViews === []
            && $this->addedFunctions === []
            && $this->removedFunctions === []
            && $this->modifiedFunctions === []
            && $this->addedProcedures === []
            && $this->removedProcedures === []
            && $this->modifiedProcedures === []
            && $this->addedDomains === []
            && $this->removedDomains === []
            && $this->modifiedDomains === []
            && $this->addedExtensions === []
            && $this->removedExtensions === []
            && $this->modifiedExtensions === [];
    }

    /**
     * @param list<MaterializedView> $materializedViews
     * @param list<View> $views
     * @param list<Table> $tables
     * @param list<Procedure> $procedures
     * @param list<Func> $functions
     * @param list<Sequence> $sequences
     * @param list<Domain> $domains
     * @param list<Extension> $extensions
     *
     * @return list<Sql>
     */
    private function dropObjectSqls(array $materializedViews, array $views, array $tables, array $procedures, array $functions, array $sequences, array $domains, array $extensions) : array
    {
        $sqls = [];

        foreach ($materializedViews as $mv) {
            $sqls[] = drop()->materializedView($mv->name);
        }

        foreach ($views as $view) {
            $sqls[] = drop()->view($view->name);
        }

        foreach ($tables as $table) {
            $sqls[] = drop()->table($table->qualifiedName())->cascade();
        }

        foreach ($procedures as $proc) {
            $sqls[] = drop()->procedure($proc->name);
        }

        foreach ($functions as $func) {
            $sqls[] = drop()->function($func->name);
        }

        foreach ($sequences as $seq) {
            $sqls[] = drop()->sequence($seq->name);
        }

        foreach ($domains as $domain) {
            $sqls[] = drop()->domain($domain->name)->cascade();
        }

        foreach ($extensions as $ext) {
            $sqls[] = drop()->extension($ext->name);
        }

        return $sqls;
    }

    /**
     * @return list<Sql>
     */
    private function modifiedSqls() : array
    {
        $sqls = [];

        $diffs = [
            $this->modifiedExtensions,
            $this->modifiedDomains,
            $this->modifiedSequences,
            $this->modifiedFunctions,
            $this->modifiedProcedures,
            $this->modifiedTables,
            $this->modifiedViews,
            $this->modifiedMaterializedViews,
        ];

        foreach ($diffs as $diffGroup) {
            foreach ($diffGroup as $diff) {
                $sqls = [...$sqls, ...$diff->generate()];
            }
        }

        return $sqls;
    }

    /**
     * @return list<Sql>
     */
    private function renamedTableSqls() : array
    {
        $sqls = [];

        foreach ($this->renamedTables as $oldQualifiedName => $newTable) {
            $sqls[] = alter()->table($oldQualifiedName)->renameTo($newTable->name);
        }

        return $sqls;
    }
}

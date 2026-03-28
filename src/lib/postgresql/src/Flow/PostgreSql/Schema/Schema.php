<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Schema;

use Flow\PostgreSql\Parser;
use Flow\PostgreSql\QueryBuilder\SqlQuery;
use Flow\PostgreSql\Schema\Exception\{SchemaException, TableNotFoundException};

final readonly class Schema
{
    /**
     * @param list<Table> $tables
     * @param list<Sequence> $sequences
     * @param list<View> $views
     * @param list<MaterializedView> $materializedViews
     * @param list<Func> $functions
     * @param list<Procedure> $procedures
     * @param list<Domain> $domains
     * @param list<Extension> $extensions
     */
    public function __construct(
        public string $name,
        public array $tables = [],
        public array $sequences = [],
        public array $views = [],
        public array $materializedViews = [],
        public array $functions = [],
        public array $procedures = [],
        public array $domains = [],
        public array $extensions = [],
    ) {
    }

    public function hasSequence(string $name) : bool
    {
        foreach ($this->sequences as $sequence) {
            if ($sequence->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function hasTable(string $name) : bool
    {
        foreach ($this->tables as $table) {
            if ($table->name === $name) {
                return true;
            }
        }

        return false;
    }

    public function sequence(string $name) : Sequence
    {
        foreach ($this->sequences as $sequence) {
            if ($sequence->name === $name) {
                return $sequence;
            }
        }

        throw new SchemaException(\sprintf('Sequence "%s" not found in schema "%s".', $name, $this->name));
    }

    public function table(string $name) : Table
    {
        foreach ($this->tables as $table) {
            if ($table->name === $name) {
                return $table;
            }
        }

        throw TableNotFoundException::inSchema($name, $this->name);
    }

    /**
     * @return list<string>
     */
    public function tableNames() : array
    {
        return \array_map(
            static fn (Table $t) : string => $t->name,
            $this->tables,
        );
    }

    /**
     * @param ExecutionOrderStrategy<Table> $tableOrderStrategy
     * @param ExecutionOrderStrategy<View> $viewOrderStrategy
     * @param ExecutionOrderStrategy<MaterializedView> $materializedViewOrderStrategy
     *
     * @return list<SqlQuery>
     */
    public function toSql(
        ExecutionOrderStrategy $tableOrderStrategy = new ForeignKeyDependencyOrder(),
        ExecutionOrderStrategy $viewOrderStrategy = new ViewDependencyOrder(new Parser()),
        ExecutionOrderStrategy $materializedViewOrderStrategy = new MaterializedViewDependencyOrder(new Parser()),
    ) : array {
        $sqls = [];

        foreach ($this->extensions as $ext) {
            $sqls[] = $ext->toSql();
        }

        foreach ($this->domains as $domain) {
            $sqls[] = $domain->toSql();
        }

        foreach ($this->sequences as $seq) {
            $sqls[] = $seq->toSql();
        }

        foreach ($this->functions as $func) {
            $sql = $func->toSql();

            if ($sql !== null) {
                $sqls[] = $sql;
            }
        }

        foreach ($this->procedures as $proc) {
            $sql = $proc->toSql();

            if ($sql !== null) {
                $sqls[] = $sql;
            }
        }

        foreach ($tableOrderStrategy->order($this->tables) as $table) {
            $sqls = [...$sqls, ...$table->toSql()];
        }

        foreach ($viewOrderStrategy->order($this->views) as $view) {
            $sqls[] = $view->toSql();
        }

        foreach ($materializedViewOrderStrategy->order($this->materializedViews) as $mv) {
            $sqls = [...$sqls, ...$mv->toSql()];
        }

        return $sqls;
    }
}

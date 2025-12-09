<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\With;

use Flow\PgQuery\QueryBuilder\Clause\WithClause;
use Flow\PgQuery\QueryBuilder\Delete\{DeleteBuilder, DeleteFromStep};
use Flow\PgQuery\QueryBuilder\Expression\Expression;
use Flow\PgQuery\QueryBuilder\Insert\{InsertBuilder, InsertIntoStep};
use Flow\PgQuery\QueryBuilder\Merge\{MergeBuilder, MergeUsingStep};
use Flow\PgQuery\QueryBuilder\Select\{SelectBuilder, SelectFromStep, SelectSelectStep};
use Flow\PgQuery\QueryBuilder\Update\{UpdateBuilder, UpdateTableStep};

final readonly class WithBuilder
{
    public function __construct(
        private WithClause $withClause,
    ) {
    }

    /**
     * Start a DELETE query with this WITH clause.
     */
    public function delete() : DeleteFromStep
    {
        return DeleteBuilder::with($this->withClause);
    }

    /**
     * Start an INSERT query with this WITH clause.
     */
    public function insert() : InsertIntoStep
    {
        return InsertBuilder::with($this->withClause);
    }

    /**
     * Start a MERGE query with this WITH clause.
     */
    public function merge(string $table, ?string $alias = null) : MergeUsingStep
    {
        return MergeBuilder::with($this->withClause)->into($table, $alias);
    }

    /**
     * Start a SELECT query with this WITH clause.
     */
    public function select(Expression ...$expressions) : SelectFromStep|SelectSelectStep
    {
        $builder = SelectBuilder::with($this->withClause);

        if ($expressions !== []) {
            return $builder->select(...$expressions);
        }

        return $builder;
    }

    /**
     * Start an UPDATE query with this WITH clause.
     */
    public function update() : UpdateTableStep
    {
        return UpdateBuilder::with($this->withClause);
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\With;

use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteBuilder;
use Flow\PostgreSql\QueryBuilder\Delete\DeleteFromStep;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Insert\InsertBuilder;
use Flow\PostgreSql\QueryBuilder\Insert\InsertIntoStep;
use Flow\PostgreSql\QueryBuilder\Merge\MergeBuilder;
use Flow\PostgreSql\QueryBuilder\Merge\MergeUsingStep;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Select\SelectFromStep;
use Flow\PostgreSql\QueryBuilder\Update\UpdateBuilder;
use Flow\PostgreSql\QueryBuilder\Update\UpdateTableStep;

final readonly class WithBuilder
{
    public function __construct(
        private WithClause $withClause,
    ) {}

    /**
     * Start a DELETE query with this WITH clause.
     */
    public function delete(): DeleteFromStep
    {
        return DeleteBuilder::with($this->withClause);
    }

    /**
     * Start an INSERT query with this WITH clause.
     */
    public function insert(): InsertIntoStep
    {
        return InsertBuilder::with($this->withClause);
    }

    /**
     * Start a MERGE query with this WITH clause.
     */
    public function merge(string $table, ?string $alias = null): MergeUsingStep
    {
        return MergeBuilder::with($this->withClause)->into($table, $alias);
    }

    /**
     * Mark this WITH clause as recursive.
     */
    public function recursive(): self
    {
        return new self(new WithClause($this->withClause->ctes(), true));
    }

    /**
     * Start a SELECT query with this WITH clause.
     */
    public function select(string|Expression ...$expressions): SelectFromStep
    {
        return SelectBuilder::with($this->withClause)->select(...$expressions);
    }

    /**
     * Start a SELECT DISTINCT query with this WITH clause.
     */
    public function selectDistinct(string|Expression ...$expressions): SelectFromStep
    {
        return SelectBuilder::with($this->withClause)->selectDistinct(...$expressions);
    }

    /**
     * Start a SELECT DISTINCT ON (...) query with this WITH clause.
     *
     * @param array<Expression|string> $distinctExpressions
     * @param Expression|string ...$selectExpressions
     */
    public function selectDistinctOn(
        array $distinctExpressions,
        string|Expression ...$selectExpressions,
    ): SelectFromStep {
        return SelectBuilder::with($this->withClause)->selectDistinctOn($distinctExpressions, ...$selectExpressions);
    }

    /**
     * Start an UPDATE query with this WITH clause.
     */
    public function update(): UpdateTableStep
    {
        return UpdateBuilder::with($this->withClause);
    }
}

<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Update;

use Flow\PgQuery\Protobuf\AST\{Alias, Node, RangeVar, ResTarget, UpdateStmt};
use Flow\PgQuery\QueryBuilder\Clause\WithClause;
use Flow\PgQuery\QueryBuilder\Condition\{Condition, ConditionFactory};
use Flow\PgQuery\QueryBuilder\Exception\{InvalidAstException, InvalidExpressionException};
use Flow\PgQuery\QueryBuilder\Expression\{Expression, ExpressionFactory, Star};
use Flow\PgQuery\QueryBuilder\Table\{JoinedTable, SubqueryReference, Table, TableFunction, TableReference};

/**
 * Builder for UPDATE statements.
 */
final readonly class UpdateBuilder implements UpdateSetStep, UpdateTableStep
{
    /**
     * @param array<string, Expression> $assignments
     * @param array<TableReference> $from
     * @param array<Expression> $returning
     */
    private function __construct(
        private ?WithClause $with = null,
        private ?string $table = null,
        private ?string $alias = null,
        private array $assignments = [],
        private array $from = [],
        private ?Condition $where = null,
        private array $returning = [],
    ) {
    }

    public static function create() : UpdateTableStep
    {
        return new self();
    }

    public static function fromAst(Node $node) : static
    {
        $updateStmt = $node->getUpdateStmt();

        if ($updateStmt === null) {
            throw InvalidAstException::unexpectedNodeType('UpdateStmt', 'unknown');
        }

        $with = null;

        if ($updateStmt->hasWithClause()) {
            $withClauseProto = $updateStmt->getWithClause();
            \assert($withClauseProto !== null);
            $withNode = new Node();
            $withNode->setWithClause($withClauseProto);
            $with = WithClause::fromAst($withNode);
        }

        $relation = $updateStmt->getRelation();

        if ($relation === null) {
            throw InvalidAstException::missingRequiredField('relation', 'UpdateStmt');
        }

        $table = $relation->getRelname();

        if ($table === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        $alias = null;

        if ($relation->hasAlias()) {
            $aliasProto = $relation->getAlias();
            \assert($aliasProto !== null);
            $alias = $aliasProto->getAliasname();
        }

        $targetList = $updateStmt->getTargetList();

        if ($targetList === null || \count($targetList) === 0) {
            throw InvalidAstException::missingRequiredField('targetList', 'UpdateStmt');
        }

        $assignments = [];

        foreach ($targetList as $targetNode) {
            $resTarget = $targetNode->getResTarget();

            if ($resTarget === null) {
                throw InvalidAstException::unexpectedNodeType('ResTarget', 'unknown');
            }

            $columnName = $resTarget->getName();

            if ($columnName === '') {
                throw InvalidAstException::missingRequiredField('name', 'ResTarget');
            }

            if (!$resTarget->hasVal()) {
                throw InvalidAstException::missingRequiredField('val', 'ResTarget');
            }

            $val = $resTarget->getVal();
            \assert($val !== null);

            $assignments[$columnName] = ExpressionFactory::fromAst($val);
        }

        $from = [];
        $fromClause = $updateStmt->getFromClause();

        if ($fromClause !== null && \count($fromClause) > 0) {
            foreach ($fromClause as $fromNode) {
                $from[] = self::tableReferenceFromAst($fromNode);
            }
        }

        $where = null;

        if ($updateStmt->hasWhereClause()) {
            $whereNode = $updateStmt->getWhereClause();
            \assert($whereNode !== null);
            $where = ConditionFactory::fromAst($whereNode);
        }

        $returning = [];
        $returningList = $updateStmt->getReturningList();

        if ($returningList !== null && \count($returningList) > 0) {
            foreach ($returningList as $returningNode) {
                $returning[] = ExpressionFactory::fromAst($returningNode);
            }
        }

        return new self($with, $table, $alias, $assignments, $from, $where, $returning);
    }

    public static function with(WithClause $with) : UpdateTableStep
    {
        return new self(with: $with);
    }

    public function from(TableReference ...$tables) : UpdateWhereStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            assignments: $this->assignments,
            from: $tables,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function returning(Expression ...$expressions) : UpdateFinalStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            assignments: $this->assignments,
            from: $this->from,
            where: $this->where,
            returning: $expressions,
        );
    }

    public function returningAll() : UpdateFinalStep
    {
        return $this->returning(Star::all());
    }

    public function set(string $column, Expression $value) : UpdateSetStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            assignments: [...$this->assignments, $column => $value],
            from: $this->from,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function setAll(array $assignments) : UpdateFromStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            assignments: [...$this->assignments, ...$assignments],
            from: $this->from,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function toAst() : UpdateStmt
    {
        if ($this->table === null || $this->table === '') {
            throw InvalidExpressionException::invalidValue('table', 'null or empty');
        }

        if ($this->assignments === []) {
            throw InvalidExpressionException::emptyArray('assignments');
        }

        $updateStmt = new UpdateStmt();

        $rangeVar = new RangeVar(['relname' => $this->table, 'inh' => true]);

        if ($this->alias !== null) {
            $aliasProto = new Alias(['aliasname' => $this->alias]);
            $rangeVar->setAlias($aliasProto);
        }

        $updateStmt->setRelation($rangeVar);

        $targetList = [];

        foreach ($this->assignments as $column => $value) {
            $resTarget = new ResTarget([
                'name' => $column,
                'val' => $value->toAst(),
            ]);

            $node = new Node();
            $node->setResTarget($resTarget);

            $targetList[] = $node;
        }

        $updateStmt->setTargetList($targetList);

        if ($this->from !== []) {
            $fromClause = [];

            foreach ($this->from as $table) {
                $fromClause[] = $table->toAst();
            }

            $updateStmt->setFromClause($fromClause);
        }

        if ($this->where !== null) {
            $updateStmt->setWhereClause($this->where->toAst());
        }

        if ($this->returning !== []) {
            $returningList = [];

            foreach ($this->returning as $expression) {
                $resTarget = new ResTarget();
                $resTarget->setVal($expression->toAst());

                $node = new Node();
                $node->setResTarget($resTarget);

                $returningList[] = $node;
            }

            $updateStmt->setReturningList($returningList);
        }

        if ($this->with !== null) {
            $withClauseNode = $this->with->toAst();
            $withClauseProto = $withClauseNode->getWithClause();
            \assert($withClauseProto !== null);
            $updateStmt->setWithClause($withClauseProto);
        }

        return $updateStmt;
    }

    public function update(string $table, ?string $alias = null) : UpdateSetStep
    {
        return new self(
            with: $this->with,
            table: $table,
            alias: $alias,
            assignments: $this->assignments,
            from: $this->from,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function where(Condition $condition) : UpdateReturningStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            assignments: $this->assignments,
            from: $this->from,
            where: $condition,
            returning: $this->returning,
        );
    }

    private static function tableReferenceFromAst(Node $node) : TableReference
    {
        if ($node->getRangeVar() !== null) {
            return Table::fromAst($node);
        }

        if ($node->getJoinExpr() !== null) {
            return JoinedTable::fromAst($node);
        }

        if ($node->getRangeSubselect() !== null) {
            return SubqueryReference::fromAst($node);
        }

        if ($node->getRangeFunction() !== null) {
            return TableFunction::fromAst($node);
        }

        throw InvalidAstException::unexpectedNodeType('table reference', 'unknown');
    }
}

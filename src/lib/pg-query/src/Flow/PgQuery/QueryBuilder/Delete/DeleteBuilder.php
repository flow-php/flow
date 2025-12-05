<?php

declare(strict_types=1);

namespace Flow\PgQuery\QueryBuilder\Delete;

use Flow\PgQuery\Protobuf\AST\{Alias, DeleteStmt, Node, RangeVar, ResTarget};
use Flow\PgQuery\QueryBuilder\Clause\WithClause;
use Flow\PgQuery\QueryBuilder\Condition\{Condition, ConditionFactory};
use Flow\PgQuery\QueryBuilder\Exception\InvalidAstException;
use Flow\PgQuery\QueryBuilder\Expression\{Expression, ExpressionFactory, Star};
use Flow\PgQuery\QueryBuilder\Table\{AliasedTable, Table, TableReference};

/**
 * Builder for DELETE statements using a fluent step-by-step API.
 *
 * Supports:
 * - DELETE FROM table
 * - WITH clause (CTEs)
 * - USING clause (multi-table deletes)
 * - WHERE clause
 * - RETURNING clause
 */
final readonly class DeleteBuilder implements DeleteFromStep, DeleteUsingStep
{
    /**
     * @param array<TableReference> $using
     * @param array<Expression> $returning
     */
    private function __construct(
        private ?WithClause $with = null,
        private ?string $table = null,
        private ?string $alias = null,
        private array $using = [],
        private ?Condition $where = null,
        private array $returning = [],
    ) {
    }

    public static function create() : DeleteFromStep
    {
        return new self();
    }

    public static function fromAst(DeleteStmt $deleteStmt) : self
    {
        $relation = $deleteStmt->getRelation();

        if ($relation === null) {
            throw InvalidAstException::missingRequiredField('relation', 'DeleteStmt');
        }

        $tableName = $relation->getRelname();

        if ($tableName === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        $alias = $relation->getAlias();
        $aliasName = $alias !== null ? $alias->getAliasname() : null;

        $withClause = null;

        if ($deleteStmt->hasWithClause()) {
            $protoWithClause = $deleteStmt->getWithClause();

            if ($protoWithClause !== null) {
                $withNode = new Node();
                $withNode->setWithClause($protoWithClause);
                $withClause = WithClause::fromAst($withNode);
            }
        }

        $using = [];
        $usingClause = $deleteStmt->getUsingClause();

        if ($usingClause !== null && \count($usingClause) > 0) {
            foreach ($usingClause as $usingNode) {
                if ($usingNode->getRangeVar() !== null) {
                    $rangeVar = $usingNode->getRangeVar();

                    if ($rangeVar->hasAlias()) {
                        $using[] = AliasedTable::fromAst($usingNode);
                    } else {
                        $using[] = Table::fromAst($usingNode);
                    }
                } else {
                    throw InvalidAstException::invalidFieldValue('using_clause', 'DeleteStmt', 'Only RangeVar nodes are supported');
                }
            }
        }

        $whereCondition = null;

        if ($deleteStmt->hasWhereClause()) {
            $whereNode = $deleteStmt->getWhereClause();

            if ($whereNode !== null) {
                $whereCondition = ConditionFactory::fromAst($whereNode);
            }
        }

        $returningExpressions = [];
        $returningList = $deleteStmt->getReturningList();

        if ($returningList !== null && \count($returningList) > 0) {
            foreach ($returningList as $resTargetNode) {
                $resTarget = $resTargetNode->getResTarget();

                if ($resTarget === null) {
                    throw InvalidAstException::invalidFieldValue('returning_list', 'DeleteStmt', 'Expected ResTarget node');
                }

                $val = $resTarget->getVal();

                if ($val === null) {
                    throw InvalidAstException::missingRequiredField('val', 'ResTarget');
                }

                $returningExpressions[] = ExpressionFactory::fromAst($val);
            }
        }

        return new self(
            with: $withClause,
            table: $tableName,
            alias: $aliasName,
            using: $using,
            where: $whereCondition,
            returning: $returningExpressions,
        );
    }

    public static function with(WithClause $with) : DeleteFromStep
    {
        return new self(with: $with);
    }

    public function from(string $table, ?string $alias = null) : DeleteUsingStep
    {
        return new self(
            with: $this->with,
            table: $table,
            alias: $alias,
            using: $this->using,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function returning(Expression ...$expressions) : DeleteFinalStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            using: $this->using,
            where: $this->where,
            returning: $expressions,
        );
    }

    public function returningAll() : DeleteFinalStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            using: $this->using,
            where: $this->where,
            returning: [Star::all()],
        );
    }

    public function toAst() : DeleteStmt
    {
        if ($this->table === null) {
            throw new \LogicException('Cannot create DeleteStmt without table name. Call from() first.');
        }

        $deleteStmt = new DeleteStmt();

        $rangeVar = new RangeVar([
            'relname' => $this->table,
            'inh' => true,
        ]);

        if ($this->alias !== null) {
            $alias = new Alias([
                'aliasname' => $this->alias,
            ]);
            $rangeVar->setAlias($alias);
        }

        $deleteStmt->setRelation($rangeVar);

        if ($this->with !== null) {
            $withNode = $this->with->toAst();
            $protoWithClause = $withNode->getWithClause();

            if ($protoWithClause !== null) {
                $deleteStmt->setWithClause($protoWithClause);
            }
        }

        if ($this->using !== []) {
            $usingNodes = [];

            foreach ($this->using as $tableRef) {
                $usingNodes[] = $tableRef->toAst();
            }

            $deleteStmt->setUsingClause($usingNodes);
        }

        if ($this->where !== null) {
            $deleteStmt->setWhereClause($this->where->toAst());
        }

        if ($this->returning !== []) {
            $returningNodes = [];

            foreach ($this->returning as $expression) {
                $resTarget = new ResTarget();
                $resTarget->setVal($expression->toAst());

                $resTargetNode = new Node();
                $resTargetNode->setResTarget($resTarget);

                $returningNodes[] = $resTargetNode;
            }

            $deleteStmt->setReturningList($returningNodes);
        }

        return $deleteStmt;
    }

    public function using(TableReference ...$tables) : DeleteWhereStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            using: $tables,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function where(Condition $condition) : DeleteReturningStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            alias: $this->alias,
            using: $this->using,
            where: $condition,
            returning: $this->returning,
        );
    }
}

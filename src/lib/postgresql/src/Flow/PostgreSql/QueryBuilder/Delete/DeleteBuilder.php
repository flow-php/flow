<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Delete;

use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\DeleteStmt;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\RangeVar;
use Flow\PostgreSql\Protobuf\AST\ResTarget;
use Flow\PostgreSql\QueryBuilder\AstToSql;
use Flow\PostgreSql\QueryBuilder\Clause\WithClause;
use Flow\PostgreSql\QueryBuilder\Condition\Condition;
use Flow\PostgreSql\QueryBuilder\Condition\ConditionFactory;
use Flow\PostgreSql\QueryBuilder\Exception\InvalidAstException;
use Flow\PostgreSql\QueryBuilder\Expression\Expression;
use Flow\PostgreSql\QueryBuilder\Expression\ExpressionFactory;
use Flow\PostgreSql\QueryBuilder\Expression\Star;
use Flow\PostgreSql\QueryBuilder\QualifiedIdentifier;
use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use Flow\PostgreSql\QueryBuilder\Table\TableReference;

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
    use AstToSql;

    /**
     * @param array<TableReference> $using
     * @param array<Expression> $returning
     */
    private function __construct(
        private ?WithClause $with = null,
        private ?string $table = null,
        private ?string $schema = null,
        private ?string $alias = null,
        private array $using = [],
        private ?Condition $where = null,
        private array $returning = [],
    ) {}

    public static function create(): DeleteFromStep
    {
        return new self();
    }

    public static function fromAst(DeleteStmt $deleteStmt): static
    {
        $relation = $deleteStmt->getRelation();

        if ($relation === null) {
            throw InvalidAstException::missingRequiredField('relation', 'DeleteStmt');
        }

        $tableName = $relation->getRelname();

        if ($tableName === '') {
            throw InvalidAstException::missingRequiredField('relname', 'RangeVar');
        }

        $schema = $relation->getSchemaname();

        if ($schema === '') {
            $schema = null;
        }

        $alias = $relation->getAlias();
        $aliasName = $alias !== null ? $alias->getAliasname() : null;

        $withClause = null;

        if ($deleteStmt->hasWithClause()) {
            $protoWithClause = $deleteStmt->getWithClause();

            if ($protoWithClause !== null) {
                $withClause = WithClause::fromAst($protoWithClause);
            }
        }

        $using = [];
        $usingClause = $deleteStmt->getUsingClause();

        if (\count($usingClause) > 0) {
            foreach ($usingClause as $usingNode) {
                $rangeVar = $usingNode->getRangeVar();

                if ($rangeVar !== null) {
                    if ($rangeVar->hasAlias()) {
                        $using[] = AliasedTable::fromAst($usingNode);
                    } else {
                        $using[] = Table::fromAst($usingNode);
                    }
                } else {
                    throw InvalidAstException::invalidFieldValue(
                        'using_clause',
                        'DeleteStmt',
                        'Only RangeVar nodes are supported',
                    );
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

        if (\count($returningList) > 0) {
            foreach ($returningList as $resTargetNode) {
                $resTarget = $resTargetNode->getResTarget();

                if ($resTarget === null) {
                    throw InvalidAstException::invalidFieldValue(
                        'returning_list',
                        'DeleteStmt',
                        'Expected ResTarget node',
                    );
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
            schema: $schema,
            alias: $aliasName,
            using: $using,
            where: $whereCondition,
            returning: $returningExpressions,
        );
    }

    public static function with(WithClause $with): DeleteFromStep
    {
        return new self(with: $with);
    }

    public function from(string|Table $table, ?string $alias = null): DeleteUsingStep
    {
        if ($table instanceof Table) {
            $name = $table->name;
            $schema = $table->schema;
        } else {
            $identifier = QualifiedIdentifier::parse($table);
            $name = $identifier->name();
            $schema = $identifier->schema();
        }

        return new self(
            with: $this->with,
            table: $name,
            schema: $schema,
            alias: $alias,
            using: $this->using,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function returning(Expression ...$expressions): DeleteFinalStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            schema: $this->schema,
            alias: $this->alias,
            using: $this->using,
            where: $this->where,
            returning: $expressions,
        );
    }

    public function returningAll(): DeleteFinalStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            schema: $this->schema,
            alias: $this->alias,
            using: $this->using,
            where: $this->where,
            returning: [Star::all()],
        );
    }

    public function toAst(): DeleteStmt
    {
        if ($this->table === null) {
            throw new \LogicException('Cannot create DeleteStmt without table name. Call from() first.');
        }

        $deleteStmt = new DeleteStmt();

        $rangeVar = new RangeVar([
            'relname' => $this->table,
            'inh' => true,
        ]);

        if ($this->schema !== null) {
            $rangeVar->setSchemaname($this->schema);
        }

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

    public function using(string|TableReference ...$tables): DeleteWhereStep
    {
        $tables = \array_map(static function (string|TableReference $t): TableReference {
            if ($t instanceof TableReference) {
                return $t;
            }
            $id = QualifiedIdentifier::parse($t);

            return new Table($id->name(), $id->schema());
        }, $tables);

        return new self(
            with: $this->with,
            table: $this->table,
            schema: $this->schema,
            alias: $this->alias,
            using: $tables,
            where: $this->where,
            returning: $this->returning,
        );
    }

    public function where(Condition $condition): DeleteReturningStep
    {
        return new self(
            with: $this->with,
            table: $this->table,
            schema: $this->schema,
            alias: $this->alias,
            using: $this->using,
            where: $condition,
            returning: $this->returning,
        );
    }
}

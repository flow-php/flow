<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\From;
use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SetOperation;

use function count;
use function Flow\Types\DSL\type_boolean;
use function iterator_to_array;

/**
 * @implements Statement<SelectStmt>
 */
final readonly class SelectStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private SelectStmt $stmt,
    ) {}

    public function from(): From
    {
        return new From(iterator_to_array($this->stmt->getFromClause()));
    }

    public function hasCte(): bool
    {
        return type_boolean()->assert($this->stmt->hasWithClause());
    }

    public function hasDataModifyingCte(): bool
    {
        foreach ($this->stmt->getWithClause()?->getCtes() ?? [] as $cte) {
            if ($cte->getCommonTableExpr()?->getCtequery()?->getSelectStmt() === null) {
                return true;
            }
        }

        return false;
    }

    public function hasIntoClause(): bool
    {
        // PostgreSQL attaches SELECT ... INTO to the first SELECT of a UNION / INTERSECT / EXCEPT and applies it to the
        // whole statement, so the INTO sits down the leftmost arm, one level per set operation
        for ($select = $this->stmt; $select !== null; $select = $select->getLarg()) {
            if (type_boolean()->assert($select->hasIntoClause())) {
                return true;
            }
        }

        return false;
    }

    public function hasLimit(): bool
    {
        return type_boolean()->assert($this->stmt->hasLimitCount());
    }

    public function hasLockingClause(): bool
    {
        return count($this->stmt->getLockingClause()) > 0;
    }

    public function hasOffset(): bool
    {
        return type_boolean()->assert($this->stmt->hasLimitOffset());
    }

    public function hasOrderBy(): bool
    {
        return count($this->stmt->getSortClause()) > 0;
    }

    public function hasSetOperation(): bool
    {
        $op = $this->stmt->getOp();

        return $op !== SetOperation::SET_OPERATION_UNDEFINED && $op !== SetOperation::SETOP_NONE;
    }

    public function raw(): SelectStmt
    {
        return $this->stmt;
    }
}

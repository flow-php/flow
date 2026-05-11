<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\From;
use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\Protobuf\AST\SetOperation;

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
        return new From(\iterator_to_array($this->stmt->getFromClause()));
    }

    public function hasCte(): bool
    {
        return $this->stmt->hasWithClause();
    }

    public function hasIntoClause(): bool
    {
        return $this->stmt->hasIntoClause();
    }

    public function hasLimit(): bool
    {
        return $this->stmt->hasLimitCount();
    }

    public function hasLockingClause(): bool
    {
        return \count($this->stmt->getLockingClause()) > 0;
    }

    public function hasOffset(): bool
    {
        return $this->stmt->hasLimitOffset();
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

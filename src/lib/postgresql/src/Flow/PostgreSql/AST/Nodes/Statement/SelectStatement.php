<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;

/**
 * @implements Statement<SelectStmt>
 */
final readonly class SelectStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private SelectStmt $stmt,
    ) {
    }

    public function hasCte() : bool
    {
        return $this->stmt->hasWithClause();
    }

    public function hasIntoClause() : bool
    {
        return $this->stmt->hasIntoClause();
    }

    public function hasLimit() : bool
    {
        return $this->stmt->hasLimitCount();
    }

    public function hasLockingClause() : bool
    {
        return \count($this->stmt->getLockingClause()) > 0;
    }

    public function hasOffset() : bool
    {
        return $this->stmt->hasLimitOffset();
    }

    public function raw() : SelectStmt
    {
        return $this->stmt;
    }

    public function toBuilder() : SelectBuilder
    {
        return SelectBuilder::fromAst($this->stmt);
    }
}

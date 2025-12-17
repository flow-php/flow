<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\LockStmt;

/**
 * @implements Statement<LockStmt>
 */
final readonly class LockStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private LockStmt $stmt,
    ) {
    }

    public function raw() : LockStmt
    {
        return $this->stmt;
    }
}

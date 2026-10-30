<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\LockStmt;

/**
 * @implements Statement<LockStmt>
 */
final readonly class LockStatement implements Statement
{
    public function __construct(
        private LockStmt $stmt,
    ) {}

    public function raw(): LockStmt
    {
        return $this->stmt;
    }
}

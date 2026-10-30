<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\ExecuteStmt;

/**
 * @implements Statement<ExecuteStmt>
 */
final readonly class ExecuteStatement implements Statement
{
    public function __construct(
        private ExecuteStmt $stmt,
    ) {}

    public function raw(): ExecuteStmt
    {
        return $this->stmt;
    }
}

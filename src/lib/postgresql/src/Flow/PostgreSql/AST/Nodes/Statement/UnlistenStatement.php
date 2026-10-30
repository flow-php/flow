<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\UnlistenStmt;

/**
 * @implements Statement<UnlistenStmt>
 */
final readonly class UnlistenStatement implements Statement
{
    public function __construct(
        private UnlistenStmt $stmt,
    ) {}

    public function raw(): UnlistenStmt
    {
        return $this->stmt;
    }
}

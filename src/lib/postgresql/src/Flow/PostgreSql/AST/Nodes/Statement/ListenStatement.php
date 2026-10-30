<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\ListenStmt;

/**
 * @implements Statement<ListenStmt>
 */
final readonly class ListenStatement implements Statement
{
    public function __construct(
        private ListenStmt $stmt,
    ) {}

    public function raw(): ListenStmt
    {
        return $this->stmt;
    }
}

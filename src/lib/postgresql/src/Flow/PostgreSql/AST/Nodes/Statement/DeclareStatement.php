<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\DeclareCursorStmt;

/**
 * @implements Statement<DeclareCursorStmt>
 */
final readonly class DeclareStatement implements Statement
{
    public function __construct(
        private DeclareCursorStmt $stmt,
    ) {}

    public function raw(): DeclareCursorStmt
    {
        return $this->stmt;
    }
}

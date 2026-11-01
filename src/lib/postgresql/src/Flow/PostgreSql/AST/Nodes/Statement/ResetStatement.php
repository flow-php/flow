<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;

/**
 * Parsed as VariableSetStmt.
 *
 * @implements Statement<VariableSetStmt>
 */
final readonly class ResetStatement implements Statement
{
    public function __construct(
        private VariableSetStmt $stmt,
    ) {}

    public function raw(): VariableSetStmt
    {
        return $this->stmt;
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\VariableShowStmt;

/**
 * Represents SHOW statements.
 *
 * @implements Statement<VariableShowStmt>
 */
final readonly class ShowStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private VariableShowStmt $stmt,
    ) {}

    public function raw(): VariableShowStmt
    {
        return $this->stmt;
    }
}

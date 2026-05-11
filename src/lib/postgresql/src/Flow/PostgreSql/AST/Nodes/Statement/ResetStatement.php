<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\VariableSetStmt;

/**
 * Represents RESET statements (RESET configuration_parameter, RESET ALL).
 * Note: These are parsed as VariableSetStmt in PostgreSQL.
 *
 * @implements Statement<VariableSetStmt>
 */
final readonly class ResetStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private VariableSetStmt $stmt,
    ) {}

    public function raw(): VariableSetStmt
    {
        return $this->stmt;
    }
}

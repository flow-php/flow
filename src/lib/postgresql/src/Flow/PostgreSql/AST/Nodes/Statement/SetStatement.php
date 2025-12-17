<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\{ConstraintsSetStmt, VariableSetStmt};

/**
 * Represents SET statements (SET configuration_parameter = value, SET ROLE, SET SESSION, etc.)
 * and SET CONSTRAINTS statements.
 *
 * @implements Statement<ConstraintsSetStmt|VariableSetStmt>
 */
final readonly class SetStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private VariableSetStmt|ConstraintsSetStmt $stmt,
    ) {
    }

    public function raw() : VariableSetStmt|ConstraintsSetStmt
    {
        return $this->stmt;
    }
}

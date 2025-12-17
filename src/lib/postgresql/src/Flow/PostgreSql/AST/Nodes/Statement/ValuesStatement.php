<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\SelectStmt;

/**
 * Represents VALUES statements (standalone VALUES clause).
 * Note: VALUES is a variant of SelectStmt in PostgreSQL.
 *
 * @implements Statement<SelectStmt>
 */
final readonly class ValuesStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private SelectStmt $stmt,
    ) {
    }

    public function raw() : SelectStmt
    {
        return $this->stmt;
    }
}

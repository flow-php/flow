<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;

/**
 * Parsed as a SelectStmt variant.
 *
 * @implements Statement<SelectStmt>
 */
final readonly class ValuesStatement implements Statement
{
    public function __construct(
        private SelectStmt $stmt,
    ) {}

    public function raw(): SelectStmt
    {
        return $this->stmt;
    }
}

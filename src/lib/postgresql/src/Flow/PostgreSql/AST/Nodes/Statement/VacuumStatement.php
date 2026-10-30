<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\VacuumStmt;

/**
 * @implements Statement<VacuumStmt>
 */
final readonly class VacuumStatement implements Statement
{
    public function __construct(
        private VacuumStmt $stmt,
    ) {}

    public function raw(): VacuumStmt
    {
        return $this->stmt;
    }
}

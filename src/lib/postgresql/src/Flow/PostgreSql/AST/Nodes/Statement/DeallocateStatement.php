<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\DeallocateStmt;

/**
 * @implements Statement<DeallocateStmt>
 */
final readonly class DeallocateStatement implements Statement
{
    public function __construct(
        private DeallocateStmt $stmt,
    ) {}

    public function raw(): DeallocateStmt
    {
        return $this->stmt;
    }
}

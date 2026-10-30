<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\UpdateStmt;

/**
 * @implements Statement<UpdateStmt>
 */
final readonly class UpdateStatement implements Statement
{
    public function __construct(
        private UpdateStmt $stmt,
    ) {}

    public function raw(): UpdateStmt
    {
        return $this->stmt;
    }
}

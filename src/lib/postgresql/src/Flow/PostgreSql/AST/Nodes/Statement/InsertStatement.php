<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\InsertStmt;

/**
 * @implements Statement<InsertStmt>
 */
final readonly class InsertStatement implements Statement
{
    public function __construct(
        private InsertStmt $stmt,
    ) {}

    public function raw(): InsertStmt
    {
        return $this->stmt;
    }
}

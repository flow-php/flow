<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\FetchStmt;

/**
 * Parsed as FetchStmt.
 *
 * @implements Statement<FetchStmt>
 */
final readonly class MoveStatement implements Statement
{
    public function __construct(
        private FetchStmt $stmt,
    ) {}

    public function raw(): FetchStmt
    {
        return $this->stmt;
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\FetchStmt;

/**
 * Represents MOVE statements.
 * Note: MOVE is treated the same as FETCH in PostgreSQL parser.
 *
 * @implements Statement<FetchStmt>
 */
final readonly class MoveStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private FetchStmt $stmt,
    ) {}

    public function raw(): FetchStmt
    {
        return $this->stmt;
    }
}

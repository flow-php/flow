<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\Protobuf\AST\TransactionStmt;

/**
 * Parsed as TransactionStmt.
 *
 * @implements Statement<TransactionStmt>
 */
final readonly class SavepointStatement implements Statement
{
    public function __construct(
        private TransactionStmt $stmt,
    ) {}

    public function raw(): TransactionStmt
    {
        return $this->stmt;
    }
}

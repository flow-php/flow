<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\TransactionStmt;

/**
 * Represents SAVEPOINT statements.
 * Note: These are parsed as TransactionStmt in PostgreSQL.
 *
 * @implements Statement<TransactionStmt>
 */
final readonly class SavepointStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private TransactionStmt $stmt,
    ) {
    }

    public function raw() : TransactionStmt
    {
        return $this->stmt;
    }
}

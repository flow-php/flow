<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\TransactionStmt;

/**
 * Represents transaction control statements: BEGIN, START TRANSACTION, COMMIT, ROLLBACK, END,
 * SAVEPOINT, RELEASE SAVEPOINT, ROLLBACK TO SAVEPOINT.
 *
 * @implements Statement<TransactionStmt>
 */
final readonly class TransactionStatement implements Statement
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

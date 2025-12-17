<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\RefreshMatViewStmt;

/**
 * Represents REFRESH MATERIALIZED VIEW statements.
 *
 * @implements Statement<RefreshMatViewStmt>
 */
final readonly class RefreshStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private RefreshMatViewStmt $stmt,
    ) {
    }

    public function raw() : RefreshMatViewStmt
    {
        return $this->stmt;
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\ReassignOwnedStmt;

/**
 * Represents REASSIGN OWNED statements.
 *
 * @implements Statement<ReassignOwnedStmt>
 */
final readonly class ReassignStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ReassignOwnedStmt $stmt,
    ) {}

    public function raw(): ReassignOwnedStmt
    {
        return $this->stmt;
    }
}

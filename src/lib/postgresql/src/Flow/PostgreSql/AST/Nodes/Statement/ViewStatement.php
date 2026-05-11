<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\ViewStmt;

/**
 * Represents CREATE VIEW statements.
 *
 * @implements Statement<ViewStmt>
 */
final readonly class ViewStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ViewStmt $stmt,
    ) {}

    public function raw(): ViewStmt
    {
        return $this->stmt;
    }
}

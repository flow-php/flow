<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\NotifyStmt;

/**
 * @implements Statement<NotifyStmt>
 */
final readonly class NotifyStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private NotifyStmt $stmt,
    ) {}

    public function raw(): NotifyStmt
    {
        return $this->stmt;
    }
}

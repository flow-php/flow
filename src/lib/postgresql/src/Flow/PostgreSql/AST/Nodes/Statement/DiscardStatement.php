<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\DiscardStmt;

/**
 * @implements Statement<DiscardStmt>
 */
final readonly class DiscardStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private DiscardStmt $stmt,
    ) {
    }

    public function raw() : DiscardStmt
    {
        return $this->stmt;
    }
}

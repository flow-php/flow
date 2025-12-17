<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\DoStmt;

/**
 * @implements Statement<DoStmt>
 */
final readonly class DoStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private DoStmt $stmt,
    ) {
    }

    public function raw() : DoStmt
    {
        return $this->stmt;
    }
}

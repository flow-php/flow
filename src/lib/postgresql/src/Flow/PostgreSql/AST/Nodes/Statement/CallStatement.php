<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\CallStmt;

/**
 * @implements Statement<CallStmt>
 */
final readonly class CallStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private CallStmt $stmt,
    ) {}

    public function raw(): CallStmt
    {
        return $this->stmt;
    }
}

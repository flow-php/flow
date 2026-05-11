<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\ExplainStmt;

/**
 * @implements Statement<ExplainStmt>
 */
final readonly class ExplainStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ExplainStmt $stmt,
    ) {}

    public function raw(): ExplainStmt
    {
        return $this->stmt;
    }
}

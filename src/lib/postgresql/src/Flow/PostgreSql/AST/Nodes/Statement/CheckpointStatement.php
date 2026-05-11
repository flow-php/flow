<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\CheckPointStmt;

/**
 * @implements Statement<CheckPointStmt>
 */
final readonly class CheckpointStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private CheckPointStmt $stmt,
    ) {}

    public function raw(): CheckPointStmt
    {
        return $this->stmt;
    }
}

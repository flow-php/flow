<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\TruncateStmt;

/**
 * @implements Statement<TruncateStmt>
 */
final readonly class TruncateStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private TruncateStmt $stmt,
    ) {}

    public function raw(): TruncateStmt
    {
        return $this->stmt;
    }
}

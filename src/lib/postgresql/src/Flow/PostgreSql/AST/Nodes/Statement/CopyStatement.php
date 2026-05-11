<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\CopyStmt;

/**
 * @implements Statement<CopyStmt>
 */
final readonly class CopyStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private CopyStmt $stmt,
    ) {}

    public function raw(): CopyStmt
    {
        return $this->stmt;
    }
}

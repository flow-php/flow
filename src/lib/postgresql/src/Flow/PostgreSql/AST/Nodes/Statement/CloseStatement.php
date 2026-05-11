<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\ClosePortalStmt;

/**
 * @implements Statement<ClosePortalStmt>
 */
final readonly class CloseStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private ClosePortalStmt $stmt,
    ) {}

    public function raw(): ClosePortalStmt
    {
        return $this->stmt;
    }
}

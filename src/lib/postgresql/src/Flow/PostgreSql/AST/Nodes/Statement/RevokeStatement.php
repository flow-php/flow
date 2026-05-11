<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\GrantRoleStmt;
use Flow\PostgreSql\Protobuf\AST\GrantStmt;

/**
 * @implements Statement<GrantRoleStmt|GrantStmt>
 */
final readonly class RevokeStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private GrantStmt|GrantRoleStmt $stmt,
    ) {}

    public function raw(): GrantStmt|GrantRoleStmt
    {
        return $this->stmt;
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\{DropOwnedStmt, DropRoleStmt, DropStmt, DropSubscriptionStmt, DropTableSpaceStmt, DropUserMappingStmt, DropdbStmt};

/**
 * Represents DROP statements (DROP TABLE, DROP INDEX, DROP ROLE, DROP DATABASE, etc.).
 *
 * @implements Statement<DropdbStmt|DropOwnedStmt|DropRoleStmt|DropStmt|DropSubscriptionStmt|DropTableSpaceStmt|DropUserMappingStmt>
 */
final readonly class DropStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private DropStmt|DropRoleStmt|DropdbStmt|DropTableSpaceStmt|DropUserMappingStmt|DropOwnedStmt|DropSubscriptionStmt $stmt,
    ) {
    }

    public function raw() : DropStmt|DropRoleStmt|DropdbStmt|DropTableSpaceStmt|DropUserMappingStmt|DropOwnedStmt|DropSubscriptionStmt
    {
        return $this->stmt;
    }
}

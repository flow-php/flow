<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\DropdbStmt;
use Flow\PostgreSql\Protobuf\AST\DropOwnedStmt;
use Flow\PostgreSql\Protobuf\AST\DropRoleStmt;
use Flow\PostgreSql\Protobuf\AST\DropStmt;
use Flow\PostgreSql\Protobuf\AST\DropSubscriptionStmt;
use Flow\PostgreSql\Protobuf\AST\DropTableSpaceStmt;
use Flow\PostgreSql\Protobuf\AST\DropUserMappingStmt;

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
    ) {}

    public function raw(): DropStmt|DropRoleStmt|DropdbStmt|DropTableSpaceStmt|DropUserMappingStmt|DropOwnedStmt|DropSubscriptionStmt
    {
        return $this->stmt;
    }
}

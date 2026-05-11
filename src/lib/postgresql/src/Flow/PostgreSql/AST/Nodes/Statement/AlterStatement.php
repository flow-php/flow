<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\Statement;
use Flow\PostgreSql\AST\Nodes\StatementTrait;
use Flow\PostgreSql\Protobuf\AST\AlterCollationStmt;
use Flow\PostgreSql\Protobuf\AST\AlterDatabaseRefreshCollStmt;
use Flow\PostgreSql\Protobuf\AST\AlterDatabaseSetStmt;
use Flow\PostgreSql\Protobuf\AST\AlterDatabaseStmt;
use Flow\PostgreSql\Protobuf\AST\AlterDefaultPrivilegesStmt;
use Flow\PostgreSql\Protobuf\AST\AlterDomainStmt;
use Flow\PostgreSql\Protobuf\AST\AlterEnumStmt;
use Flow\PostgreSql\Protobuf\AST\AlterEventTrigStmt;
use Flow\PostgreSql\Protobuf\AST\AlterExtensionContentsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterExtensionStmt;
use Flow\PostgreSql\Protobuf\AST\AlterFdwStmt;
use Flow\PostgreSql\Protobuf\AST\AlterForeignServerStmt;
use Flow\PostgreSql\Protobuf\AST\AlterFunctionStmt;
use Flow\PostgreSql\Protobuf\AST\AlterObjectDependsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterObjectSchemaStmt;
use Flow\PostgreSql\Protobuf\AST\AlterOperatorStmt;
use Flow\PostgreSql\Protobuf\AST\AlterOpFamilyStmt;
use Flow\PostgreSql\Protobuf\AST\AlterOwnerStmt;
use Flow\PostgreSql\Protobuf\AST\AlterPolicyStmt;
use Flow\PostgreSql\Protobuf\AST\AlterPublicationStmt;
use Flow\PostgreSql\Protobuf\AST\AlterRoleSetStmt;
use Flow\PostgreSql\Protobuf\AST\AlterRoleStmt;
use Flow\PostgreSql\Protobuf\AST\AlterSeqStmt;
use Flow\PostgreSql\Protobuf\AST\AlterStatsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterSubscriptionStmt;
use Flow\PostgreSql\Protobuf\AST\AlterSystemStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableMoveAllStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableSpaceOptionsStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTableStmt;
use Flow\PostgreSql\Protobuf\AST\AlterTypeStmt;
use Flow\PostgreSql\Protobuf\AST\AlterUserMappingStmt;
use Flow\PostgreSql\Protobuf\AST\RenameStmt;

/**
 * Represents ALTER statements (ALTER TABLE, ALTER INDEX, ALTER ROLE, ALTER DATABASE, etc.).
 *
 * @implements Statement<AlterCollationStmt|AlterDatabaseRefreshCollStmt|AlterDatabaseSetStmt|AlterDatabaseStmt|AlterDefaultPrivilegesStmt|AlterDomainStmt|AlterEnumStmt|AlterEventTrigStmt|AlterExtensionContentsStmt|AlterExtensionStmt|AlterFdwStmt|AlterForeignServerStmt|AlterFunctionStmt|AlterObjectDependsStmt|AlterObjectSchemaStmt|AlterOperatorStmt|AlterOpFamilyStmt|AlterOwnerStmt|AlterPolicyStmt|AlterPublicationStmt|AlterRoleSetStmt|AlterRoleStmt|AlterSeqStmt|AlterStatsStmt|AlterSubscriptionStmt|AlterSystemStmt|AlterTableMoveAllStmt|AlterTableSpaceOptionsStmt|AlterTableStmt|AlterTypeStmt|AlterUserMappingStmt|RenameStmt>
 */
final readonly class AlterStatement implements Statement
{
    use StatementTrait;

    public function __construct(
        private AlterTableStmt|AlterDomainStmt|AlterFunctionStmt|AlterRoleStmt|AlterRoleSetStmt|AlterDatabaseStmt|AlterDatabaseSetStmt|AlterDatabaseRefreshCollStmt|AlterSeqStmt|AlterOwnerStmt|AlterObjectSchemaStmt|AlterObjectDependsStmt|AlterExtensionStmt|AlterExtensionContentsStmt|AlterFdwStmt|AlterForeignServerStmt|AlterUserMappingStmt|AlterTableSpaceOptionsStmt|AlterTableMoveAllStmt|AlterPolicyStmt|AlterPublicationStmt|AlterSubscriptionStmt|AlterDefaultPrivilegesStmt|AlterCollationStmt|AlterEnumStmt|AlterOperatorStmt|AlterOpFamilyStmt|AlterTypeStmt|AlterSystemStmt|AlterEventTrigStmt|AlterStatsStmt|RenameStmt $stmt,
    ) {}

    public function raw(): AlterTableStmt|AlterDomainStmt|AlterFunctionStmt|AlterRoleStmt|AlterRoleSetStmt|AlterDatabaseStmt|AlterDatabaseSetStmt|AlterDatabaseRefreshCollStmt|AlterSeqStmt|AlterOwnerStmt|AlterObjectSchemaStmt|AlterObjectDependsStmt|AlterExtensionStmt|AlterExtensionContentsStmt|AlterFdwStmt|AlterForeignServerStmt|AlterUserMappingStmt|AlterTableSpaceOptionsStmt|AlterTableMoveAllStmt|AlterPolicyStmt|AlterPublicationStmt|AlterSubscriptionStmt|AlterDefaultPrivilegesStmt|AlterCollationStmt|AlterEnumStmt|AlterOperatorStmt|AlterOpFamilyStmt|AlterTypeStmt|AlterSystemStmt|AlterEventTrigStmt|AlterStatsStmt|RenameStmt
    {
        return $this->stmt;
    }
}

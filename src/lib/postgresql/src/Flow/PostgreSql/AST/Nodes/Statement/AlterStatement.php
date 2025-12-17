<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes\Statement;

use Flow\PostgreSql\AST\Nodes\{Statement, StatementTrait};
use Flow\PostgreSql\Protobuf\AST\{AlterCollationStmt, AlterDatabaseRefreshCollStmt, AlterDatabaseSetStmt, AlterDatabaseStmt, AlterDefaultPrivilegesStmt, AlterDomainStmt, AlterEnumStmt, AlterEventTrigStmt, AlterExtensionContentsStmt, AlterExtensionStmt, AlterFdwStmt, AlterForeignServerStmt, AlterFunctionStmt, AlterObjectDependsStmt, AlterObjectSchemaStmt, AlterOpFamilyStmt, AlterOperatorStmt, AlterOwnerStmt, AlterPolicyStmt, AlterPublicationStmt, AlterRoleSetStmt, AlterRoleStmt, AlterSeqStmt, AlterStatsStmt, AlterSubscriptionStmt, AlterSystemStmt, AlterTableMoveAllStmt, AlterTableSpaceOptionsStmt, AlterTableStmt, AlterTypeStmt, AlterUserMappingStmt, RenameStmt};

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
    ) {
    }

    public function raw() : AlterTableStmt|AlterDomainStmt|AlterFunctionStmt|AlterRoleStmt|AlterRoleSetStmt|AlterDatabaseStmt|AlterDatabaseSetStmt|AlterDatabaseRefreshCollStmt|AlterSeqStmt|AlterOwnerStmt|AlterObjectSchemaStmt|AlterObjectDependsStmt|AlterExtensionStmt|AlterExtensionContentsStmt|AlterFdwStmt|AlterForeignServerStmt|AlterUserMappingStmt|AlterTableSpaceOptionsStmt|AlterTableMoveAllStmt|AlterPolicyStmt|AlterPublicationStmt|AlterSubscriptionStmt|AlterDefaultPrivilegesStmt|AlterCollationStmt|AlterEnumStmt|AlterOperatorStmt|AlterOpFamilyStmt|AlterTypeStmt|AlterSystemStmt|AlterEventTrigStmt|AlterStatsStmt|RenameStmt
    {
        return $this->stmt;
    }
}

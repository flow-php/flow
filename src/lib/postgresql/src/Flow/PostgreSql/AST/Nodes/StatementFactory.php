<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Statement\{AlterStatement, CallStatement, CheckpointStatement, CloseStatement, ClusterStatement, CommentStatement, CopyStatement, CreateStatement, DeallocateStatement, DeclareStatement, DeleteStatement, DiscardStatement, DoStatement, DropStatement, ExecuteStatement, ExplainStatement, FetchStatement, GrantStatement, ImportStatement, IndexStatement, InsertStatement, ListenStatement, LoadStatement, LockStatement, MergeStatement, NotifyStatement, PrepareStatement, ReassignStatement, RefreshStatement, ReindexStatement, RevokeStatement, RuleStatement, SecurityLabelStatement, SelectStatement, SetStatement, ShowStatement, TransactionStatement, TruncateStatement, UnknownStatement, UnlistenStatement, UpdateStatement, VacuumStatement, ViewStatement};
use Flow\PostgreSql\Protobuf\AST\Node;

final class StatementFactory
{
    /**
     * @return Statement<mixed>
     */
    public static function fromNode(Node $node) : Statement
    {
        return match (true) {
            $node->getSelectStmt() !== null => new SelectStatement($node->getSelectStmt()),
            $node->getInsertStmt() !== null => new InsertStatement($node->getInsertStmt()),
            $node->getUpdateStmt() !== null => new UpdateStatement($node->getUpdateStmt()),
            $node->getDeleteStmt() !== null => new DeleteStatement($node->getDeleteStmt()),
            $node->getMergeStmt() !== null => new MergeStatement($node->getMergeStmt()),

            $node->getTransactionStmt() !== null => new TransactionStatement($node->getTransactionStmt()),

            $node->getCreateStmt() !== null => new CreateStatement($node->getCreateStmt()),
            $node->getCreateSchemaStmt() !== null => new CreateStatement($node->getCreateSchemaStmt()),
            $node->getCreateSeqStmt() !== null => new CreateStatement($node->getCreateSeqStmt()),
            $node->getCreateTableAsStmt() !== null => new CreateStatement($node->getCreateTableAsStmt()),
            $node->getCreateTableSpaceStmt() !== null => new CreateStatement($node->getCreateTableSpaceStmt()),
            $node->getCreateFunctionStmt() !== null => new CreateStatement($node->getCreateFunctionStmt()),
            $node->getCreateTrigStmt() !== null => new CreateStatement($node->getCreateTrigStmt()),
            $node->getCreateRoleStmt() !== null => new CreateStatement($node->getCreateRoleStmt()),
            $node->getCreateDomainStmt() !== null => new CreateStatement($node->getCreateDomainStmt()),
            $node->getCreateEnumStmt() !== null => new CreateStatement($node->getCreateEnumStmt()),
            $node->getCreateRangeStmt() !== null => new CreateStatement($node->getCreateRangeStmt()),
            $node->getCreateExtensionStmt() !== null => new CreateStatement($node->getCreateExtensionStmt()),
            $node->getCreateFdwStmt() !== null => new CreateStatement($node->getCreateFdwStmt()),
            $node->getCreateForeignServerStmt() !== null => new CreateStatement($node->getCreateForeignServerStmt()),
            $node->getCreateForeignTableStmt() !== null => new CreateStatement($node->getCreateForeignTableStmt()),
            $node->getCreateUserMappingStmt() !== null => new CreateStatement($node->getCreateUserMappingStmt()),
            $node->getCreatePolicyStmt() !== null => new CreateStatement($node->getCreatePolicyStmt()),
            $node->getCreateAmStmt() !== null => new CreateStatement($node->getCreateAmStmt()),
            $node->getCreatePublicationStmt() !== null => new CreateStatement($node->getCreatePublicationStmt()),
            $node->getCreateSubscriptionStmt() !== null => new CreateStatement($node->getCreateSubscriptionStmt()),
            $node->getCreateStatsStmt() !== null => new CreateStatement($node->getCreateStatsStmt()),
            $node->getCreateCastStmt() !== null => new CreateStatement($node->getCreateCastStmt()),
            $node->getCreateConversionStmt() !== null => new CreateStatement($node->getCreateConversionStmt()),
            $node->getCreateOpClassStmt() !== null => new CreateStatement($node->getCreateOpClassStmt()),
            $node->getCreateOpFamilyStmt() !== null => new CreateStatement($node->getCreateOpFamilyStmt()),
            $node->getCreatePlangStmt() !== null => new CreateStatement($node->getCreatePlangStmt()),
            $node->getCreateTransformStmt() !== null => new CreateStatement($node->getCreateTransformStmt()),
            $node->getCreateEventTrigStmt() !== null => new CreateStatement($node->getCreateEventTrigStmt()),
            $node->getCreatedbStmt() !== null => new CreateStatement($node->getCreatedbStmt()),
            $node->getCompositeTypeStmt() !== null => new CreateStatement($node->getCompositeTypeStmt()),

            $node->getAlterTableStmt() !== null => new AlterStatement($node->getAlterTableStmt()),
            $node->getAlterDomainStmt() !== null => new AlterStatement($node->getAlterDomainStmt()),
            $node->getAlterFunctionStmt() !== null => new AlterStatement($node->getAlterFunctionStmt()),
            $node->getAlterRoleStmt() !== null => new AlterStatement($node->getAlterRoleStmt()),
            $node->getAlterRoleSetStmt() !== null => new AlterStatement($node->getAlterRoleSetStmt()),
            $node->getAlterDatabaseStmt() !== null => new AlterStatement($node->getAlterDatabaseStmt()),
            $node->getAlterDatabaseSetStmt() !== null => new AlterStatement($node->getAlterDatabaseSetStmt()),
            $node->getAlterDatabaseRefreshCollStmt() !== null => new AlterStatement($node->getAlterDatabaseRefreshCollStmt()),
            $node->getAlterSeqStmt() !== null => new AlterStatement($node->getAlterSeqStmt()),
            $node->getAlterOwnerStmt() !== null => new AlterStatement($node->getAlterOwnerStmt()),
            $node->getAlterObjectSchemaStmt() !== null => new AlterStatement($node->getAlterObjectSchemaStmt()),
            $node->getAlterObjectDependsStmt() !== null => new AlterStatement($node->getAlterObjectDependsStmt()),
            $node->getAlterExtensionStmt() !== null => new AlterStatement($node->getAlterExtensionStmt()),
            $node->getAlterExtensionContentsStmt() !== null => new AlterStatement($node->getAlterExtensionContentsStmt()),
            $node->getAlterFdwStmt() !== null => new AlterStatement($node->getAlterFdwStmt()),
            $node->getAlterForeignServerStmt() !== null => new AlterStatement($node->getAlterForeignServerStmt()),
            $node->getAlterUserMappingStmt() !== null => new AlterStatement($node->getAlterUserMappingStmt()),
            $node->getAlterTableSpaceOptionsStmt() !== null => new AlterStatement($node->getAlterTableSpaceOptionsStmt()),
            $node->getAlterTableMoveAllStmt() !== null => new AlterStatement($node->getAlterTableMoveAllStmt()),
            $node->getAlterPolicyStmt() !== null => new AlterStatement($node->getAlterPolicyStmt()),
            $node->getAlterPublicationStmt() !== null => new AlterStatement($node->getAlterPublicationStmt()),
            $node->getAlterSubscriptionStmt() !== null => new AlterStatement($node->getAlterSubscriptionStmt()),
            $node->getAlterDefaultPrivilegesStmt() !== null => new AlterStatement($node->getAlterDefaultPrivilegesStmt()),
            $node->getAlterCollationStmt() !== null => new AlterStatement($node->getAlterCollationStmt()),
            $node->getAlterEnumStmt() !== null => new AlterStatement($node->getAlterEnumStmt()),
            $node->getAlterOperatorStmt() !== null => new AlterStatement($node->getAlterOperatorStmt()),
            $node->getAlterOpFamilyStmt() !== null => new AlterStatement($node->getAlterOpFamilyStmt()),
            $node->getAlterTypeStmt() !== null => new AlterStatement($node->getAlterTypeStmt()),
            $node->getAlterSystemStmt() !== null => new AlterStatement($node->getAlterSystemStmt()),
            $node->getAlterEventTrigStmt() !== null => new AlterStatement($node->getAlterEventTrigStmt()),
            $node->getAlterStatsStmt() !== null => new AlterStatement($node->getAlterStatsStmt()),
            $node->getRenameStmt() !== null => new AlterStatement($node->getRenameStmt()),

            $node->getDropStmt() !== null => new DropStatement($node->getDropStmt()),
            $node->getDropRoleStmt() !== null => new DropStatement($node->getDropRoleStmt()),
            $node->getDropdbStmt() !== null => new DropStatement($node->getDropdbStmt()),
            $node->getDropTableSpaceStmt() !== null => new DropStatement($node->getDropTableSpaceStmt()),
            $node->getDropUserMappingStmt() !== null => new DropStatement($node->getDropUserMappingStmt()),
            $node->getDropOwnedStmt() !== null => new DropStatement($node->getDropOwnedStmt()),
            $node->getDropSubscriptionStmt() !== null => new DropStatement($node->getDropSubscriptionStmt()),

            $node->getTruncateStmt() !== null => new TruncateStatement($node->getTruncateStmt()),
            $node->getIndexStmt() !== null => new IndexStatement($node->getIndexStmt()),
            $node->getViewStmt() !== null => new ViewStatement($node->getViewStmt()),
            $node->getRuleStmt() !== null => new RuleStatement($node->getRuleStmt()),

            $node->getCopyStmt() !== null => new CopyStatement($node->getCopyStmt()),
            $node->getExplainStmt() !== null => new ExplainStatement($node->getExplainStmt()),
            $node->getVacuumStmt() !== null => new VacuumStatement($node->getVacuumStmt()),
            $node->getLockStmt() !== null => new LockStatement($node->getLockStmt()),
            $node->getReindexStmt() !== null => new ReindexStatement($node->getReindexStmt()),
            $node->getClusterStmt() !== null => new ClusterStatement($node->getClusterStmt()),
            $node->getCheckPointStmt() !== null => new CheckpointStatement($node->getCheckPointStmt()),
            $node->getLoadStmt() !== null => new LoadStatement($node->getLoadStmt()),
            $node->getDiscardStmt() !== null => new DiscardStatement($node->getDiscardStmt()),
            $node->getDoStmt() !== null => new DoStatement($node->getDoStmt()),
            $node->getCallStmt() !== null => new CallStatement($node->getCallStmt()),
            $node->getRefreshMatViewStmt() !== null => new RefreshStatement($node->getRefreshMatViewStmt()),
            $node->getReassignOwnedStmt() !== null => new ReassignStatement($node->getReassignOwnedStmt()),
            $node->getImportForeignSchemaStmt() !== null => new ImportStatement($node->getImportForeignSchemaStmt()),

            $node->getGrantStmt() !== null => self::createGrantOrRevokeStatement($node),
            $node->getGrantRoleStmt() !== null => self::createGrantRoleStatement($node),

            $node->getPrepareStmt() !== null => new PrepareStatement($node->getPrepareStmt()),
            $node->getExecuteStmt() !== null => new ExecuteStatement($node->getExecuteStmt()),
            $node->getDeallocateStmt() !== null => new DeallocateStatement($node->getDeallocateStmt()),

            $node->getDeclareCursorStmt() !== null => new DeclareStatement($node->getDeclareCursorStmt()),
            $node->getFetchStmt() !== null => new FetchStatement($node->getFetchStmt()),
            $node->getClosePortalStmt() !== null => new CloseStatement($node->getClosePortalStmt()),

            $node->getListenStmt() !== null => new ListenStatement($node->getListenStmt()),
            $node->getUnlistenStmt() !== null => new UnlistenStatement($node->getUnlistenStmt()),
            $node->getNotifyStmt() !== null => new NotifyStatement($node->getNotifyStmt()),

            $node->getVariableSetStmt() !== null => new SetStatement($node->getVariableSetStmt()),
            $node->getVariableShowStmt() !== null => new ShowStatement($node->getVariableShowStmt()),
            $node->getConstraintsSetStmt() !== null => new SetStatement($node->getConstraintsSetStmt()),

            $node->getSecLabelStmt() !== null => new SecurityLabelStatement($node->getSecLabelStmt()),
            $node->getCommentStmt() !== null => new CommentStatement($node->getCommentStmt()),

            $node->getDefineStmt() !== null => new CreateStatement($node->getDefineStmt()),

            default => new UnknownStatement($node),
        };
    }

    /**
     * @return Statement<mixed>
     */
    private static function createGrantOrRevokeStatement(Node $node) : Statement
    {
        $grantStmt = $node->getGrantStmt();

        if ($grantStmt !== null && $grantStmt->getIsGrant()) {
            return new GrantStatement($grantStmt);
        }

        if ($grantStmt !== null) {
            return new RevokeStatement($grantStmt);
        }

        throw new \LogicException('createGrantOrRevokeStatement called without GrantStmt');
    }

    /**
     * @return Statement<mixed>
     */
    private static function createGrantRoleStatement(Node $node) : Statement
    {
        $grantRoleStmt = $node->getGrantRoleStmt();

        if ($grantRoleStmt !== null && $grantRoleStmt->getIsGrant()) {
            return new GrantStatement($grantRoleStmt);
        }

        if ($grantRoleStmt !== null) {
            return new RevokeStatement($grantRoleStmt);
        }

        throw new \LogicException('createGrantRoleStatement called without GrantRoleStmt');
    }
}

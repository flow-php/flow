<?php

declare(strict_types=1);

namespace Flow\PostgreSql\AST\Nodes;

use Flow\PostgreSql\AST\Nodes\Statement\AlterStatement;
use Flow\PostgreSql\AST\Nodes\Statement\CallStatement;
use Flow\PostgreSql\AST\Nodes\Statement\CheckpointStatement;
use Flow\PostgreSql\AST\Nodes\Statement\CloseStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ClusterStatement;
use Flow\PostgreSql\AST\Nodes\Statement\CommentStatement;
use Flow\PostgreSql\AST\Nodes\Statement\CopyStatement;
use Flow\PostgreSql\AST\Nodes\Statement\CreateStatement;
use Flow\PostgreSql\AST\Nodes\Statement\DeallocateStatement;
use Flow\PostgreSql\AST\Nodes\Statement\DeclareStatement;
use Flow\PostgreSql\AST\Nodes\Statement\DeleteStatement;
use Flow\PostgreSql\AST\Nodes\Statement\DiscardStatement;
use Flow\PostgreSql\AST\Nodes\Statement\DoStatement;
use Flow\PostgreSql\AST\Nodes\Statement\DropStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ExecuteStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ExplainStatement;
use Flow\PostgreSql\AST\Nodes\Statement\FetchStatement;
use Flow\PostgreSql\AST\Nodes\Statement\GrantStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ImportStatement;
use Flow\PostgreSql\AST\Nodes\Statement\IndexStatement;
use Flow\PostgreSql\AST\Nodes\Statement\InsertStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ListenStatement;
use Flow\PostgreSql\AST\Nodes\Statement\LoadStatement;
use Flow\PostgreSql\AST\Nodes\Statement\LockStatement;
use Flow\PostgreSql\AST\Nodes\Statement\MergeStatement;
use Flow\PostgreSql\AST\Nodes\Statement\NotifyStatement;
use Flow\PostgreSql\AST\Nodes\Statement\PrepareStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ReassignStatement;
use Flow\PostgreSql\AST\Nodes\Statement\RefreshStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ReindexStatement;
use Flow\PostgreSql\AST\Nodes\Statement\RevokeStatement;
use Flow\PostgreSql\AST\Nodes\Statement\RuleStatement;
use Flow\PostgreSql\AST\Nodes\Statement\SecurityLabelStatement;
use Flow\PostgreSql\AST\Nodes\Statement\SelectStatement;
use Flow\PostgreSql\AST\Nodes\Statement\SetStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ShowStatement;
use Flow\PostgreSql\AST\Nodes\Statement\TransactionStatement;
use Flow\PostgreSql\AST\Nodes\Statement\TruncateStatement;
use Flow\PostgreSql\AST\Nodes\Statement\UnknownStatement;
use Flow\PostgreSql\AST\Nodes\Statement\UnlistenStatement;
use Flow\PostgreSql\AST\Nodes\Statement\UpdateStatement;
use Flow\PostgreSql\AST\Nodes\Statement\VacuumStatement;
use Flow\PostgreSql\AST\Nodes\Statement\ViewStatement;
use Flow\PostgreSql\Protobuf\AST\Node;

final class StatementFactory
{
    /**
     * @return Statement<mixed>
     */
    public static function fromNode(Node $node): Statement
    {
        return match (true) {
            ($s = $node->getSelectStmt()) !== null => new SelectStatement($s),
            ($s = $node->getInsertStmt()) !== null => new InsertStatement($s),
            ($s = $node->getUpdateStmt()) !== null => new UpdateStatement($s),
            ($s = $node->getDeleteStmt()) !== null => new DeleteStatement($s),
            ($s = $node->getMergeStmt()) !== null => new MergeStatement($s),
            ($s = $node->getTransactionStmt()) !== null => new TransactionStatement($s),
            ($s = $node->getCreateStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateSchemaStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateSeqStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateTableAsStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateTableSpaceStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateFunctionStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateTrigStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateRoleStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateDomainStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateEnumStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateRangeStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateExtensionStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateFdwStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateForeignServerStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateForeignTableStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateUserMappingStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreatePolicyStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateAmStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreatePublicationStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateSubscriptionStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateStatsStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateCastStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateConversionStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateOpClassStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateOpFamilyStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreatePlangStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateTransformStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreateEventTrigStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCreatedbStmt()) !== null => new CreateStatement($s),
            ($s = $node->getCompositeTypeStmt()) !== null => new CreateStatement($s),
            ($s = $node->getAlterTableStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterDomainStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterFunctionStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterRoleStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterRoleSetStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterDatabaseStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterDatabaseSetStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterDatabaseRefreshCollStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterSeqStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterOwnerStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterObjectSchemaStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterObjectDependsStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterExtensionStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterExtensionContentsStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterFdwStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterForeignServerStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterUserMappingStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterTableSpaceOptionsStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterTableMoveAllStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterPolicyStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterPublicationStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterSubscriptionStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterDefaultPrivilegesStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterCollationStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterEnumStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterOperatorStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterOpFamilyStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterTypeStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterSystemStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterEventTrigStmt()) !== null => new AlterStatement($s),
            ($s = $node->getAlterStatsStmt()) !== null => new AlterStatement($s),
            ($s = $node->getRenameStmt()) !== null => new AlterStatement($s),
            ($s = $node->getDropStmt()) !== null => new DropStatement($s),
            ($s = $node->getDropRoleStmt()) !== null => new DropStatement($s),
            ($s = $node->getDropdbStmt()) !== null => new DropStatement($s),
            ($s = $node->getDropTableSpaceStmt()) !== null => new DropStatement($s),
            ($s = $node->getDropUserMappingStmt()) !== null => new DropStatement($s),
            ($s = $node->getDropOwnedStmt()) !== null => new DropStatement($s),
            ($s = $node->getDropSubscriptionStmt()) !== null => new DropStatement($s),
            ($s = $node->getTruncateStmt()) !== null => new TruncateStatement($s),
            ($s = $node->getIndexStmt()) !== null => new IndexStatement($s),
            ($s = $node->getViewStmt()) !== null => new ViewStatement($s),
            ($s = $node->getRuleStmt()) !== null => new RuleStatement($s),
            ($s = $node->getCopyStmt()) !== null => new CopyStatement($s),
            ($s = $node->getExplainStmt()) !== null => new ExplainStatement($s),
            ($s = $node->getVacuumStmt()) !== null => new VacuumStatement($s),
            ($s = $node->getLockStmt()) !== null => new LockStatement($s),
            ($s = $node->getReindexStmt()) !== null => new ReindexStatement($s),
            ($s = $node->getClusterStmt()) !== null => new ClusterStatement($s),
            ($s = $node->getCheckPointStmt()) !== null => new CheckpointStatement($s),
            ($s = $node->getLoadStmt()) !== null => new LoadStatement($s),
            ($s = $node->getDiscardStmt()) !== null => new DiscardStatement($s),
            ($s = $node->getDoStmt()) !== null => new DoStatement($s),
            ($s = $node->getCallStmt()) !== null => new CallStatement($s),
            ($s = $node->getRefreshMatViewStmt()) !== null => new RefreshStatement($s),
            ($s = $node->getReassignOwnedStmt()) !== null => new ReassignStatement($s),
            ($s = $node->getImportForeignSchemaStmt()) !== null => new ImportStatement($s),
            $node->getGrantStmt() !== null => self::createGrantOrRevokeStatement($node),
            $node->getGrantRoleStmt() !== null => self::createGrantRoleStatement($node),
            ($s = $node->getPrepareStmt()) !== null => new PrepareStatement($s),
            ($s = $node->getExecuteStmt()) !== null => new ExecuteStatement($s),
            ($s = $node->getDeallocateStmt()) !== null => new DeallocateStatement($s),
            ($s = $node->getDeclareCursorStmt()) !== null => new DeclareStatement($s),
            ($s = $node->getFetchStmt()) !== null => new FetchStatement($s),
            ($s = $node->getClosePortalStmt()) !== null => new CloseStatement($s),
            ($s = $node->getListenStmt()) !== null => new ListenStatement($s),
            ($s = $node->getUnlistenStmt()) !== null => new UnlistenStatement($s),
            ($s = $node->getNotifyStmt()) !== null => new NotifyStatement($s),
            ($s = $node->getVariableSetStmt()) !== null => new SetStatement($s),
            ($s = $node->getVariableShowStmt()) !== null => new ShowStatement($s),
            ($s = $node->getConstraintsSetStmt()) !== null => new SetStatement($s),
            ($s = $node->getSecLabelStmt()) !== null => new SecurityLabelStatement($s),
            ($s = $node->getCommentStmt()) !== null => new CommentStatement($s),
            ($s = $node->getDefineStmt()) !== null => new CreateStatement($s),
            default => new UnknownStatement($node),
        };
    }

    /**
     * @return Statement<mixed>
     */
    private static function createGrantOrRevokeStatement(Node $node): Statement
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
    private static function createGrantRoleStatement(Node $node): Statement
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

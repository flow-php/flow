<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Assertions;

use function Flow\PgQuery\DSL\sql_parse;
use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{AlterDomainStmt, AlterEnumStmt, AlterExtensionContentsStmt, AlterExtensionStmt, AlterFunctionStmt, AlterObjectDependsStmt, AlterObjectSchemaStmt, AlterOwnerStmt, AlterRoleStmt, AlterSeqStmt, AlterTableStmt, CallStmt, ClusterStmt, CommentStmt, CompositeTypeStmt, CreateDomainStmt, CreateEnumStmt, CreateExtensionStmt, CreateFunctionStmt, CreateRangeStmt, CreateRoleStmt, CreateSchemaStmt, CreateSeqStmt, CreateStmt, CreateTableAsStmt, CreateTrigStmt, DeleteStmt, DiscardStmt, DoStmt, DropOwnedStmt, DropRoleStmt, DropStmt, ExplainStmt, GrantRoleStmt, GrantStmt, IndexStmt, InsertStmt, LockStmt, Node, RawStmt, ReassignOwnedStmt, RefreshMatViewStmt, ReindexStmt, RenameStmt, RuleStmt, SelectStmt, TransactionStmt, TruncateStmt, UpdateStmt, VacuumStmt, VariableSetStmt, ViewStmt};
use Flow\PgQuery\QueryBuilder\Delete\{DeleteBuilder, DeleteFinalStep};
use Flow\PgQuery\QueryBuilder\Insert\{InsertBuilder, InsertFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\AlterSequence\{AlterSequenceLoggingFinalStep, AlterSequenceOptionsStep, AlterSequenceOwnerFinalStep, AlterSequenceSchemaFinalStep, RenameSequenceFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\AlterTable\{AlterTableFinalStep, AlterTableSchemaBuilder, RenameTableBuilder};
use Flow\PgQuery\QueryBuilder\Schema\CreateSequence\CreateSequenceOptionsStep;
use Flow\PgQuery\QueryBuilder\Schema\CreateTable\CreateTableFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\CreateTableAs\CreateTableAsFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Domain\{AlterDomainFinalStep, CreateDomainOptionsStep, DropDomainFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\DropSequence\DropSequenceFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\DropTable\DropTableFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Extension\{AlterExtensionFinalStep, CreateExtensionOptionsStep, DropExtensionFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Function\{
    AlterFunctionFinalStep,
    AlterProcedureFinalStep,
    CallFinalStep,
    CreateFunctionFinalStep,
    CreateProcedureFinalStep,
    DoFinalStep,
    DropFunctionFinalStep,
    DropProcedureFinalStep
};
use Flow\PgQuery\QueryBuilder\Schema\Grant\{GrantFinalStep, GrantRoleFinalStep, RevokeFinalStep, RevokeRoleFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Index\AlterIndex\{AlterTablespaceIndexFinalStep, RenameIndexFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Index\CreateIndex\CreateIndexFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Index\DropIndex\DropIndexFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Index\Reindex\ReindexFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Ownership\{DropOwnedFinalStep, ReassignOwnedFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Role\{AlterRoleFinalStep, AlterRoleRenameFinalStep, CreateRoleFinalStep, DropRoleFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Rule\{CreateRuleFinalStep, DropRuleFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Schema\{AlterSchemaOwnerFinalStep, AlterSchemaRenameFinalStep, CreateSchemaFinalStep, DropSchemaFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Session\{ResetRoleFinalStep, SetRoleFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Trigger\{AlterTriggerFinalStep, CreateTriggerFinalStep, DropTriggerFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\Truncate\TruncateFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Type\{AlterEnumTypeFinalStep, CreateCompositeTypeFinalStep, CreateEnumTypeFinalStep, CreateRangeTypeFinalStep, DropTypeFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\View\AlterMaterializedView\{AlterMatViewOwnerFinalStep, AlterMatViewSchemaFinalStep, AlterMatViewTablespaceFinalStep, RenameMatViewFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\View\AlterView\{AlterViewOwnerFinalStep, AlterViewSchemaFinalStep, RenameViewFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\View\CreateMaterializedView\CreateMatViewFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\View\CreateView\CreateViewFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\View\DropMaterializedView\DropMatViewFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\View\DropView\DropViewFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\View\RefreshMaterializedView\RefreshMatViewFinalStep;
use Flow\PgQuery\QueryBuilder\Select\{SelectBuilder, SelectFinalStep};
use Flow\PgQuery\QueryBuilder\Transaction\{
    BeginFinalStep,
    CommitFinalStep,
    PreparedTransactionFinalStep,
    RollbackFinalStep,
    SavepointFinalStep,
    SetTransactionFinalStep
};
use Flow\PgQuery\QueryBuilder\Update\{UpdateBuilder, UpdateFinalStep};
use Flow\PgQuery\QueryBuilder\Utility\{AnalyzeFinalStep, ClusterFinalStep, CommentFinalStep, DiscardFinalStep, ExplainFinalStep, LockFinalStep, VacuumFinalStep};

use PHPUnit\Framework\Assert;

trait QueryBuilderAssertions
{
    protected function assertAlterDomainQuery(AlterDomainFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterDomainStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterEnumTypeQuery(AlterEnumTypeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterEnumStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterExtensionQuery(AlterExtensionFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();

        if ($ast instanceof AlterExtensionStmt) {
            $sql = $this->deparseAlterExtensionStmt($ast);
        } else {
            $sql = $this->deparseAlterExtensionContentsStmt($ast);
        }

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterFunctionQuery(AlterFunctionFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();
        Assert::assertInstanceOf(AlterFunctionStmt::class, $ast);
        $sql = $this->deparseAlterFunctionStmt($ast);

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterFunctionRenameQuery(AlterFunctionFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();
        Assert::assertInstanceOf(RenameStmt::class, $ast);
        $sql = $this->deparseRenameStmt($ast);

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterIndexRenameQuery(RenameIndexFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterIndexTablespaceQuery(AlterTablespaceIndexFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterMaterializedViewOwnerQuery(AlterMatViewOwnerFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterMaterializedViewRenameQuery(RenameMatViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterMaterializedViewSchemaQuery(AlterMatViewSchemaFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterObjectSchemaStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterMaterializedViewTablespaceQuery(AlterMatViewTablespaceFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterObjectSchemaQuery(AlterTableSchemaBuilder $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterObjectSchemaStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterProcedureQuery(AlterProcedureFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();
        Assert::assertInstanceOf(AlterFunctionStmt::class, $ast);
        $sql = $this->deparseAlterFunctionStmt($ast);

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterProcedureRenameQuery(AlterProcedureFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();
        Assert::assertInstanceOf(RenameStmt::class, $ast);
        $sql = $this->deparseRenameStmt($ast);

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterRoleQuery(AlterRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterRoleStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterRoleRenameQuery(AlterRoleRenameFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSchemaOwnerQuery(AlterSchemaOwnerFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterOwnerStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSchemaRenameQuery(AlterSchemaRenameFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSequenceLoggingQuery(AlterSequenceLoggingFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSequenceOwnerQuery(AlterSequenceOwnerFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSequenceQuery(AlterSequenceOptionsStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterSeqStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSequenceRenameQuery(RenameSequenceFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterSequenceSchemaQuery(AlterSequenceSchemaFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterObjectSchemaStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterTableQuery(AlterTableFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterTriggerDependsQuery(AlterTriggerFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();
        Assert::assertInstanceOf(AlterObjectDependsStmt::class, $ast);
        $sql = $this->deparseAlterObjectDependsStmt($ast);

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterTriggerRenameQuery(AlterTriggerFinalStep $builder, string $expectedSql) : void
    {
        $ast = $builder->toAst();
        Assert::assertInstanceOf(RenameStmt::class, $ast);
        $sql = $this->deparseRenameStmt($ast);

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterViewOwnerQuery(AlterViewOwnerFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterViewRenameQuery(RenameViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterViewSchemaQuery(AlterViewSchemaFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterObjectSchemaStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAnalyzeQuery(AnalyzeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseVacuumStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCallQuery(CallFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCallStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertClusterQuery(ClusterFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseClusterStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCommentQuery(CommentFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCommentStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateCompositeTypeQuery(CreateCompositeTypeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCompositeTypeStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateDomainQuery(CreateDomainOptionsStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateDomainStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateEnumTypeQuery(CreateEnumTypeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateEnumStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateExtensionQuery(CreateExtensionOptionsStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateExtensionStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateFunctionQuery(CreateFunctionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateFunctionStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateIndexQuery(CreateIndexFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseIndexStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateMaterializedViewQuery(CreateMatViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateTableAsStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateProcedureQuery(CreateProcedureFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateFunctionStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateRangeTypeQuery(CreateRangeTypeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateRangeStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateRoleQuery(CreateRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateRoleStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateRuleQuery(CreateRuleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRuleStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateSchemaQuery(CreateSchemaFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateSchemaStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateSequenceQuery(CreateSequenceOptionsStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateSeqStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateTableAsQuery(CreateTableAsFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateTableAsStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateTableQuery(CreateTableFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateTriggerQuery(CreateTriggerFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseCreateTrigStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertCreateViewQuery(CreateViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseViewStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDeleteQueryRoundTrip(DeleteFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDeleteStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = sql_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = DeleteBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseDeleteStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertDiscardQuery(DiscardFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDiscardStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDoQuery(DoFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDoStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropDomainQuery(DropDomainFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropExtensionQuery(DropExtensionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropFunctionQuery(DropFunctionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropIndexQuery(DropIndexFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropMaterializedViewQuery(DropMatViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropOwnedQuery(DropOwnedFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropOwnedStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropProcedureQuery(DropProcedureFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropRoleQuery(DropRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropRoleStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropRuleQuery(DropRuleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropSchemaQuery(DropSchemaFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropSequenceQuery(DropSequenceFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropTableQuery(DropTableFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropTriggerQuery(DropTriggerFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropTypeQuery(DropTypeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertDropViewQuery(DropViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertExplainQuery(ExplainFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseExplainStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertGrantQuery(GrantFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseGrantStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertGrantRoleQuery(GrantRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseGrantRoleStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertInsertQueryRoundTrip(InsertFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseInsertStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = sql_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = InsertBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseInsertStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertLockQuery(LockFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseLockStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertReassignOwnedQuery(ReassignOwnedFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseReassignOwnedStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertRefreshMaterializedViewQuery(RefreshMatViewFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRefreshMatViewStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertReindexQuery(ReindexFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseReindexStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertRenameQuery(RenameTableBuilder $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertResetRoleQuery(ResetRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseVariableSetStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertRevokeQuery(RevokeFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseGrantStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertRevokeRoleQuery(RevokeRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseGrantRoleStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertSelectQueryRoundTrip(SelectFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseSelectStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = sql_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = SelectBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseSelectStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertSetRoleQuery(SetRoleFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseVariableSetStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertSetTransactionQueryEquals(SetTransactionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseVariableSetStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertTransactionQueryEquals(BeginFinalStep|CommitFinalStep|RollbackFinalStep|SavepointFinalStep|PreparedTransactionFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseTransactionStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertTruncateQuery(TruncateFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseTruncateStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertUpdateQueryRoundTrip(UpdateFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseUpdateStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = sql_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = UpdateBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseUpdateStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertVacuumQuery(VacuumFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseVacuumStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function deparseAlterDomainStmt(AlterDomainStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_domain_stmt' => $stmt]));
    }

    protected function deparseAlterEnumStmt(AlterEnumStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_enum_stmt' => $stmt]));
    }

    protected function deparseAlterExtensionContentsStmt(AlterExtensionContentsStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_extension_contents_stmt' => $stmt]));
    }

    protected function deparseAlterExtensionStmt(AlterExtensionStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_extension_stmt' => $stmt]));
    }

    protected function deparseAlterFunctionStmt(AlterFunctionStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_function_stmt' => $stmt]));
    }

    protected function deparseAlterObjectDependsStmt(AlterObjectDependsStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_object_depends_stmt' => $stmt]));
    }

    protected function deparseAlterObjectSchemaStmt(AlterObjectSchemaStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_object_schema_stmt' => $stmt]));
    }

    protected function deparseAlterOwnerStmt(AlterOwnerStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_owner_stmt' => $stmt]));
    }

    protected function deparseAlterRoleStmt(AlterRoleStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_role_stmt' => $stmt]));
    }

    protected function deparseAlterSeqStmt(AlterSeqStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_seq_stmt' => $stmt]));
    }

    protected function deparseAlterTableStmt(AlterTableStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_table_stmt' => $stmt]));
    }

    protected function deparseCallStmt(CallStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['call_stmt' => $stmt]));
    }

    protected function deparseClusterStmt(ClusterStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['cluster_stmt' => $stmt]));
    }

    protected function deparseCommentStmt(CommentStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['comment_stmt' => $stmt]));
    }

    protected function deparseCompositeTypeStmt(CompositeTypeStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['composite_type_stmt' => $stmt]));
    }

    protected function deparseCreateDomainStmt(CreateDomainStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_domain_stmt' => $stmt]));
    }

    protected function deparseCreateEnumStmt(CreateEnumStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_enum_stmt' => $stmt]));
    }

    protected function deparseCreateExtensionStmt(CreateExtensionStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_extension_stmt' => $stmt]));
    }

    protected function deparseCreateFunctionStmt(CreateFunctionStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_function_stmt' => $stmt]));
    }

    protected function deparseCreateRangeStmt(CreateRangeStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_range_stmt' => $stmt]));
    }

    protected function deparseCreateRoleStmt(CreateRoleStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_role_stmt' => $stmt]));
    }

    protected function deparseCreateSchemaStmt(CreateSchemaStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_schema_stmt' => $stmt]));
    }

    protected function deparseCreateSeqStmt(CreateSeqStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_seq_stmt' => $stmt]));
    }

    protected function deparseCreateStmt(CreateStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_stmt' => $stmt]));
    }

    protected function deparseCreateTableAsStmt(CreateTableAsStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_table_as_stmt' => $stmt]));
    }

    protected function deparseCreateTrigStmt(CreateTrigStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_trig_stmt' => $stmt]));
    }

    protected function deparseDeleteStmt(DeleteStmt $deleteStmt) : string
    {
        return $this->deparseNode(new Node(['delete_stmt' => $deleteStmt]));
    }

    protected function deparseDiscardStmt(DiscardStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['discard_stmt' => $stmt]));
    }

    protected function deparseDoStmt(DoStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['do_stmt' => $stmt]));
    }

    protected function deparseDropOwnedStmt(DropOwnedStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['drop_owned_stmt' => $stmt]));
    }

    protected function deparseDropRoleStmt(DropRoleStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['drop_role_stmt' => $stmt]));
    }

    protected function deparseDropStmt(DropStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['drop_stmt' => $stmt]));
    }

    protected function deparseExplainStmt(ExplainStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['explain_stmt' => $stmt]));
    }

    protected function deparseGrantRoleStmt(GrantRoleStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['grant_role_stmt' => $stmt]));
    }

    protected function deparseGrantStmt(GrantStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['grant_stmt' => $stmt]));
    }

    protected function deparseIndexStmt(IndexStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['index_stmt' => $stmt]));
    }

    protected function deparseInsertStmt(InsertStmt $insertStmt) : string
    {
        return $this->deparseNode(new Node(['insert_stmt' => $insertStmt]));
    }

    protected function deparseLockStmt(LockStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['lock_stmt' => $stmt]));
    }

    protected function deparseReassignOwnedStmt(ReassignOwnedStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['reassign_owned_stmt' => $stmt]));
    }

    protected function deparseRefreshMatViewStmt(RefreshMatViewStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['refresh_mat_view_stmt' => $stmt]));
    }

    protected function deparseReindexStmt(ReindexStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['reindex_stmt' => $stmt]));
    }

    protected function deparseRenameStmt(RenameStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['rename_stmt' => $stmt]));
    }

    protected function deparseRuleStmt(RuleStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['rule_stmt' => $stmt]));
    }

    protected function deparseSelectStmt(SelectStmt $selectStmt) : string
    {
        return $this->deparseNode(new Node(['select_stmt' => $selectStmt]));
    }

    protected function deparseTransactionStmt(TransactionStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['transaction_stmt' => $stmt]));
    }

    protected function deparseTruncateStmt(TruncateStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['truncate_stmt' => $stmt]));
    }

    protected function deparseUpdateStmt(UpdateStmt $updateStmt) : string
    {
        return $this->deparseNode(new Node(['update_stmt' => $updateStmt]));
    }

    protected function deparseVacuumStmt(VacuumStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['vacuum_stmt' => $stmt]));
    }

    protected function deparseVariableSetStmt(VariableSetStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['variable_set_stmt' => $stmt]));
    }

    protected function deparseViewStmt(ViewStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['view_stmt' => $stmt]));
    }

    private function deparseNode(Node $node) : string
    {
        $parser = new Parser();
        $rawStmt = new RawStmt(['stmt' => $node]);
        $parsed = $parser->parse('SELECT 1');
        $parseResult = $parsed->raw();
        $parseResult->setStmts([$rawStmt]);

        return (new ParsedQuery($parseResult))->deparse();
    }
}

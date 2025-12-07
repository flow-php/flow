<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Assertions;

use function Flow\PgQuery\DSL\sql_parse;
use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{AlterObjectSchemaStmt, AlterTableStmt, CreateStmt, CreateTableAsStmt, DeleteStmt, DropStmt, InsertStmt, Node, RawStmt, RenameStmt, SelectStmt, TruncateStmt, UpdateStmt};
use Flow\PgQuery\QueryBuilder\Delete\{DeleteBuilder, DeleteFinalStep};
use Flow\PgQuery\QueryBuilder\Insert\{InsertBuilder, InsertFinalStep};
use Flow\PgQuery\QueryBuilder\Schema\AlterTable\{AlterTableFinalStep, AlterTableSchemaBuilder, RenameTableBuilder};
use Flow\PgQuery\QueryBuilder\Schema\CreateTable\CreateTableFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\CreateTableAs\CreateTableAsFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\DropTable\DropTableFinalStep;
use Flow\PgQuery\QueryBuilder\Schema\Truncate\TruncateFinalStep;
use Flow\PgQuery\QueryBuilder\Select\{SelectBuilder, SelectFinalStep};
use Flow\PgQuery\QueryBuilder\Update\{UpdateBuilder, UpdateFinalStep};

use PHPUnit\Framework\Assert;

trait QueryBuilderAssertions
{
    protected function assertAlterObjectSchemaQuery(AlterTableSchemaBuilder $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterObjectSchemaStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);
    }

    protected function assertAlterTableQuery(AlterTableFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseAlterTableStmt($builder->toAst());

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

    protected function assertDropTableQuery(DropTableFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDropStmt($builder->toAst());

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

    protected function assertRenameQuery(RenameTableBuilder $builder, string $expectedSql) : void
    {
        $sql = $this->deparseRenameStmt($builder->toAst());

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

    protected function deparseAlterObjectSchemaStmt(AlterObjectSchemaStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_object_schema_stmt' => $stmt]));
    }

    protected function deparseAlterTableStmt(AlterTableStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['alter_table_stmt' => $stmt]));
    }

    protected function deparseCreateStmt(CreateStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_stmt' => $stmt]));
    }

    protected function deparseCreateTableAsStmt(CreateTableAsStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['create_table_as_stmt' => $stmt]));
    }

    protected function deparseDeleteStmt(DeleteStmt $deleteStmt) : string
    {
        return $this->deparseNode(new Node(['delete_stmt' => $deleteStmt]));
    }

    protected function deparseDropStmt(DropStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['drop_stmt' => $stmt]));
    }

    protected function deparseInsertStmt(InsertStmt $insertStmt) : string
    {
        return $this->deparseNode(new Node(['insert_stmt' => $insertStmt]));
    }

    protected function deparseRenameStmt(RenameStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['rename_stmt' => $stmt]));
    }

    protected function deparseSelectStmt(SelectStmt $selectStmt) : string
    {
        return $this->deparseNode(new Node(['select_stmt' => $selectStmt]));
    }

    protected function deparseTruncateStmt(TruncateStmt $stmt) : string
    {
        return $this->deparseNode(new Node(['truncate_stmt' => $stmt]));
    }

    protected function deparseUpdateStmt(UpdateStmt $updateStmt) : string
    {
        return $this->deparseNode(new Node(['update_stmt' => $updateStmt]));
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

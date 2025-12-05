<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder\Assertions;

use function Flow\PgQuery\DSL\pg_parse;
use Flow\PgQuery\{ParsedQuery, Parser};
use Flow\PgQuery\Protobuf\AST\{DeleteStmt, InsertStmt, Node, RawStmt, SelectStmt, UpdateStmt};
use Flow\PgQuery\QueryBuilder\Delete\{DeleteBuilder, DeleteFinalStep};
use Flow\PgQuery\QueryBuilder\Insert\{InsertBuilder, InsertFinalStep};
use Flow\PgQuery\QueryBuilder\Select\{SelectBuilder, SelectFinalStep};
use Flow\PgQuery\QueryBuilder\Update\{UpdateBuilder, UpdateFinalStep};

use PHPUnit\Framework\Assert;

trait QueryBuilderAssertions
{
    protected function assertDeleteQueryRoundTrip(DeleteFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseDeleteStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = pg_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $deleteStmt = $rebuiltStmt->getDeleteStmt();
        Assert::assertNotNull($deleteStmt);

        $rebuilt = DeleteBuilder::fromAst($deleteStmt);
        $rebuiltSql = $this->deparseDeleteStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertInsertQueryRoundTrip(InsertFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseInsertStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = pg_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = InsertBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseInsertStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertSelectQueryRoundTrip(SelectFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseSelectStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = pg_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = SelectBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseSelectStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function assertUpdateQueryRoundTrip(UpdateFinalStep $builder, string $expectedSql) : void
    {
        $sql = $this->deparseUpdateStmt($builder->toAst());

        Assert::assertSame($expectedSql, $sql);

        $parsed = pg_parse($sql);
        $stmts = $parsed->raw()->getStmts();
        Assert::assertCount(1, $stmts);

        $rebuiltStmt = $stmts[0]->getStmt();
        Assert::assertNotNull($rebuiltStmt);

        $rebuilt = UpdateBuilder::fromAst($rebuiltStmt);
        $rebuiltSql = $this->deparseUpdateStmt($rebuilt->toAst());

        Assert::assertSame($sql, $rebuiltSql);
    }

    protected function deparseDeleteStmt(DeleteStmt $deleteStmt) : string
    {
        return $this->deparseNode(new Node(['delete_stmt' => $deleteStmt]));
    }

    protected function deparseInsertStmt(InsertStmt $insertStmt) : string
    {
        return $this->deparseNode(new Node(['insert_stmt' => $insertStmt]));
    }

    protected function deparseSelectStmt(SelectStmt $selectStmt) : string
    {
        return $this->deparseNode(new Node(['select_stmt' => $selectStmt]));
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

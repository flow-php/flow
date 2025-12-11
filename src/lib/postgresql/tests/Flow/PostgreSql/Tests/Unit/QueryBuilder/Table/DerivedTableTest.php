<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\{Node, SelectStmt};
use Flow\PostgreSql\QueryBuilder\Table\{AliasedTable, DerivedTable};
use PHPUnit\Framework\TestCase;

final class DerivedTableTest extends TestCase
{
    public function test_as_method_returns_aliased_table() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq');

        $aliased = $derived->as('sq');

        self::assertInstanceOf(AliasedTable::class, $aliased);
        self::assertSame('sq', $aliased->alias);
    }

    public function test_converts_derived_table_to_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq');

        $node = $derived->toAst();

        self::assertTrue($node->hasRangeSubselect());

        $rangeSubselect = $node->getRangeSubselect();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeSubselect::class, $rangeSubselect);

        $alias = $rangeSubselect->getAlias();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Alias::class, $alias);
        self::assertSame('subq', $alias->getAliasname());
        self::assertCount(0, $alias->getColnames());

        self::assertFalse($rangeSubselect->getLateral());

        $subqueryNode = $rangeSubselect->getSubquery();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Node::class, $subqueryNode);
        self::assertTrue($subqueryNode->hasSelectStmt());
    }

    public function test_converts_derived_table_with_column_aliases_to_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', ['id', 'name']);

        $node = $derived->toAst();

        $rangeSubselect = $node->getRangeSubselect();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeSubselect::class, $rangeSubselect);
        $alias = $rangeSubselect->getAlias();

        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\Alias::class, $alias);
        self::assertSame('subq', $alias->getAliasname());

        $colnames = $alias->getColnames();
        self::assertCount(2, $colnames);

        $col1 = $colnames[0]->getString();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col1);
        self::assertSame('id', $col1->getSval());

        $col2 = $colnames[1]->getString();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\PBString::class, $col2);
        self::assertSame('name', $col2->getSval());
    }

    public function test_converts_lateral_derived_table_to_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', null, true);

        $node = $derived->toAst();

        $rangeSubselect = $node->getRangeSubselect();
        self::assertInstanceOf(\Flow\PostgreSql\Protobuf\AST\RangeSubselect::class, $rangeSubselect);
        self::assertTrue($rangeSubselect->getLateral());
    }

    public function test_creates_derived_table() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq');

        self::assertSame($subquery, $derived->subquery);
        self::assertSame('subq', $derived->alias);
        self::assertNull($derived->columnAliases);
        self::assertFalse($derived->lateral);
    }

    public function test_creates_derived_table_with_column_aliases() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', ['id', 'name']);

        self::assertSame(['id', 'name'], $derived->columnAliases);
    }

    public function test_creates_lateral_derived_table() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', null, true);

        self::assertTrue($derived->lateral);
    }

    public function test_reconstructs_derived_table_from_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new DerivedTable($subquery, 'subq');

        $node = $original->toAst();
        $reconstructed = DerivedTable::fromAst($node);

        self::assertInstanceOf(DerivedTable::class, $reconstructed);
        self::assertSame('subq', $reconstructed->alias);
        self::assertNull($reconstructed->columnAliases);
        self::assertFalse($reconstructed->lateral);
    }

    public function test_reconstructs_derived_table_with_column_aliases_from_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new DerivedTable($subquery, 'subq', ['id', 'name']);

        $node = $original->toAst();
        $reconstructed = DerivedTable::fromAst($node);

        self::assertSame(['id', 'name'], $reconstructed->columnAliases);
    }

    public function test_reconstructs_lateral_derived_table_from_ast() : void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new DerivedTable($subquery, 'subq', null, true);

        $node = $original->toAst();
        $reconstructed = DerivedTable::fromAst($node);

        self::assertTrue($reconstructed->lateral);
    }
}

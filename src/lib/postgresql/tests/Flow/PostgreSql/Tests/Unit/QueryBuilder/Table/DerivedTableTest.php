<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Table;

use Flow\PostgreSql\Protobuf\AST\Alias;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\PBString;
use Flow\PostgreSql\Protobuf\AST\RangeSubselect;
use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Table\AliasedTable;
use Flow\PostgreSql\QueryBuilder\Table\DerivedTable;
use PHPUnit\Framework\TestCase;

final class DerivedTableTest extends TestCase
{
    public function test_as_method_returns_aliased_table(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq');

        $aliased = $derived->as('sq');

        static::assertInstanceOf(AliasedTable::class, $aliased);
        static::assertSame('sq', $aliased->alias);
    }

    public function test_converts_derived_table_to_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq');

        $node = $derived->toAst();

        static::assertTrue($node->hasRangeSubselect());

        $rangeSubselect = $node->getRangeSubselect();
        static::assertInstanceOf(RangeSubselect::class, $rangeSubselect);

        $alias = $rangeSubselect->getAlias();
        static::assertInstanceOf(Alias::class, $alias);
        static::assertSame('subq', $alias->getAliasname());
        static::assertCount(0, $alias->getColnames());

        static::assertFalse($rangeSubselect->getLateral());

        $subqueryNode = $rangeSubselect->getSubquery();
        static::assertInstanceOf(Node::class, $subqueryNode);
        static::assertTrue($subqueryNode->hasSelectStmt());
    }

    public function test_converts_derived_table_with_column_aliases_to_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', ['id', 'name']);

        $node = $derived->toAst();

        $rangeSubselect = $node->getRangeSubselect();
        static::assertInstanceOf(RangeSubselect::class, $rangeSubselect);
        $alias = $rangeSubselect->getAlias();

        static::assertInstanceOf(Alias::class, $alias);
        static::assertSame('subq', $alias->getAliasname());

        $colnames = $alias->getColnames();
        static::assertCount(2, $colnames);

        $col1 = $colnames[0]->getString();
        static::assertInstanceOf(PBString::class, $col1);
        static::assertSame('id', $col1->getSval());

        $col2 = $colnames[1]->getString();
        static::assertInstanceOf(PBString::class, $col2);
        static::assertSame('name', $col2->getSval());
    }

    public function test_converts_lateral_derived_table_to_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', null, true);

        $node = $derived->toAst();

        $rangeSubselect = $node->getRangeSubselect();
        static::assertInstanceOf(RangeSubselect::class, $rangeSubselect);
        static::assertTrue($rangeSubselect->getLateral());
    }

    public function test_creates_derived_table(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq');

        static::assertSame($subquery, $derived->subquery);
        static::assertSame('subq', $derived->alias);
        static::assertNull($derived->columnAliases);
        static::assertFalse($derived->lateral);
    }

    public function test_creates_derived_table_with_column_aliases(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', ['id', 'name']);

        static::assertSame(['id', 'name'], $derived->columnAliases);
    }

    public function test_creates_lateral_derived_table(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $derived = new DerivedTable($subquery, 'subq', null, true);

        static::assertTrue($derived->lateral);
    }

    public function test_reconstructs_derived_table_from_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new DerivedTable($subquery, 'subq');

        $node = $original->toAst();
        $reconstructed = DerivedTable::fromAst($node);

        static::assertInstanceOf(DerivedTable::class, $reconstructed);
        static::assertSame('subq', $reconstructed->alias);
        static::assertNull($reconstructed->columnAliases);
        static::assertFalse($reconstructed->lateral);
    }

    public function test_reconstructs_derived_table_with_column_aliases_from_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new DerivedTable($subquery, 'subq', ['id', 'name']);

        $node = $original->toAst();
        $reconstructed = DerivedTable::fromAst($node);

        static::assertSame(['id', 'name'], $reconstructed->columnAliases);
    }

    public function test_reconstructs_lateral_derived_table_from_ast(): void
    {
        $subquery = new Node(['select_stmt' => new SelectStmt()]);
        $original = new DerivedTable($subquery, 'subq', null, true);

        $node = $original->toAst();
        $reconstructed = DerivedTable::fromAst($node);

        static::assertTrue($reconstructed->lateral);
    }
}

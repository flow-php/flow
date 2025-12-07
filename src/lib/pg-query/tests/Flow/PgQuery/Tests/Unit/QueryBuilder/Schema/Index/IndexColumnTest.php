<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PgQuery\Protobuf\AST\{IndexElem, SortByDir, SortByNulls};
use Flow\PgQuery\QueryBuilder\Expression\RawExpression;
use Flow\PgQuery\QueryBuilder\Schema\Index\IndexColumn;
use PHPUnit\Framework\TestCase;

final class IndexColumnTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_asc_sets_ordering() : void
    {
        $column = IndexColumn::column('name')->asc();

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertSame(SortByDir::SORTBY_ASC, $ast->getOrdering());
    }

    public function test_collation() : void
    {
        $column = IndexColumn::column('name')->collate('en_US');

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertCount(1, $ast->getCollation());
        self::assertSame('en_US', $ast->getCollation()[0]->getString()->getSval());
    }

    public function test_column_creates_index_elem() : void
    {
        $column = IndexColumn::column('name');

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertSame('name', $ast->getName());
    }

    public function test_combined_options() : void
    {
        $column = IndexColumn::column('name')
            ->desc()
            ->nullsFirst()
            ->opclass('text_pattern_ops')
            ->collate('en_US');

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertSame('name', $ast->getName());
        self::assertSame(SortByDir::SORTBY_DESC, $ast->getOrdering());
        self::assertSame(SortByNulls::SORTBY_NULLS_FIRST, $ast->getNullsOrdering());
        self::assertCount(1, $ast->getOpclass());
        self::assertCount(1, $ast->getCollation());
    }

    public function test_desc_sets_ordering() : void
    {
        $column = IndexColumn::column('name')->desc();

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertSame(SortByDir::SORTBY_DESC, $ast->getOrdering());
    }

    public function test_expression_creates_index_elem() : void
    {
        $column = IndexColumn::expression(new RawExpression('lower(name)'));

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertEmpty($ast->getName());
        self::assertNotNull($ast->getExpr());
    }

    public function test_immutability() : void
    {
        $original = IndexColumn::column('name');
        $modified = $original->desc();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        self::assertSame(SortByDir::SORT_BY_DIR_UNDEFINED, $originalAst->getOrdering());
        self::assertSame(SortByDir::SORTBY_DESC, $modifiedAst->getOrdering());
    }

    public function test_nulls_first_sets_nulls_ordering() : void
    {
        $column = IndexColumn::column('name')->nullsFirst();

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertSame(SortByNulls::SORTBY_NULLS_FIRST, $ast->getNullsOrdering());
    }

    public function test_nulls_last_sets_nulls_ordering() : void
    {
        $column = IndexColumn::column('name')->nullsLast();

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertSame(SortByNulls::SORTBY_NULLS_LAST, $ast->getNullsOrdering());
    }

    public function test_opclass() : void
    {
        $column = IndexColumn::column('name')->opclass('text_pattern_ops');

        $ast = $column->toAst();

        self::assertInstanceOf(IndexElem::class, $ast);
        self::assertCount(1, $ast->getOpclass());
        self::assertSame('text_pattern_ops', $ast->getOpclass()[0]->getString()->getSval());
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Schema\Index;

use Flow\PostgreSql\Protobuf\AST\IndexElem;
use Flow\PostgreSql\Protobuf\AST\Node;
use Flow\PostgreSql\Protobuf\AST\SortByDir;
use Flow\PostgreSql\Protobuf\AST\SortByNulls;
use Flow\PostgreSql\QueryBuilder\Schema\Index\IndexColumn;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\func;
use function Flow\Types\DSL\type_instance_of;

final class IndexColumnTest extends TestCase
{
    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded.');
        }
    }

    public function test_asc_sets_ordering(): void
    {
        $column = IndexColumn::column('name')->asc();

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertSame(SortByDir::SORTBY_ASC, $ast->getOrdering());
    }

    public function test_collation(): void
    {
        $column = IndexColumn::column('name')->collate('en_US');

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        $collation = $ast->getCollation();
        static::assertCount(1, $collation);
        $first = type_instance_of(Node::class)->assert($collation[0]);
        static::assertSame('en_US', $first->getString()?->getSval());
    }

    public function test_column_creates_index_elem(): void
    {
        $column = IndexColumn::column('name');

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertSame('name', $ast->getName());
    }

    public function test_combined_options(): void
    {
        $column = IndexColumn::column('name')->desc()->nullsFirst()->opclass('text_pattern_ops')->collate('en_US');

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertSame('name', $ast->getName());
        static::assertSame(SortByDir::SORTBY_DESC, $ast->getOrdering());
        static::assertSame(SortByNulls::SORTBY_NULLS_FIRST, $ast->getNullsOrdering());
        static::assertCount(1, $ast->getOpclass());
        static::assertCount(1, $ast->getCollation());
    }

    public function test_desc_sets_ordering(): void
    {
        $column = IndexColumn::column('name')->desc();

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertSame(SortByDir::SORTBY_DESC, $ast->getOrdering());
    }

    public function test_expression_creates_index_elem(): void
    {
        $column = IndexColumn::expression(func('lower', [col('name')]));

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertEmpty($ast->getName());
        static::assertNotNull($ast->getExpr());
    }

    public function test_immutability(): void
    {
        $original = IndexColumn::column('name');
        $modified = $original->desc();

        $originalAst = $original->toAst();
        $modifiedAst = $modified->toAst();

        static::assertSame(SortByDir::SORT_BY_DIR_UNDEFINED, $originalAst->getOrdering());
        static::assertSame(SortByDir::SORTBY_DESC, $modifiedAst->getOrdering());
    }

    public function test_nulls_first_sets_nulls_ordering(): void
    {
        $column = IndexColumn::column('name')->nullsFirst();

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertSame(SortByNulls::SORTBY_NULLS_FIRST, $ast->getNullsOrdering());
    }

    public function test_nulls_last_sets_nulls_ordering(): void
    {
        $column = IndexColumn::column('name')->nullsLast();

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        static::assertSame(SortByNulls::SORTBY_NULLS_LAST, $ast->getNullsOrdering());
    }

    public function test_opclass(): void
    {
        $column = IndexColumn::column('name')->opclass('text_pattern_ops');

        $ast = $column->toAst();

        static::assertInstanceOf(IndexElem::class, $ast);
        $opclass = $ast->getOpclass();
        static::assertCount(1, $opclass);
        $first = type_instance_of(Node::class)->assert($opclass[0]);
        static::assertSame('text_pattern_ops', $first->getString()?->getSval());
    }
}

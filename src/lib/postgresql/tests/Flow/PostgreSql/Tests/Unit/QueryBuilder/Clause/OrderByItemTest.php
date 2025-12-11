<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\{SortBy, SortByDir, SortByNulls};
use Flow\PostgreSql\QueryBuilder\Clause\{NullsPosition, OrderByItem, SortDirection};
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use PHPUnit\Framework\TestCase;

final class OrderByItemTest extends TestCase
{
    public function test_asc_creates_new_instance_with_ascending_direction() : void
    {
        $column = Column::name('name');
        $orderBy = new OrderByItem($column);

        $ascOrderBy = $orderBy->asc();

        self::assertNotSame($orderBy, $ascOrderBy);
        self::assertSame(SortDirection::ASC, $ascOrderBy->direction());
        self::assertSame($column, $ascOrderBy->expression());
    }

    public function test_constructor_with_all_parameters() : void
    {
        $column = Column::name('priority');
        $orderBy = new OrderByItem($column, SortDirection::DESC, NullsPosition::FIRST);

        self::assertSame($column, $orderBy->expression());
        self::assertSame(SortDirection::DESC, $orderBy->direction());
        self::assertSame(NullsPosition::FIRST, $orderBy->nulls());
    }

    public function test_constructor_with_default_parameters() : void
    {
        $column = Column::name('name');
        $orderBy = new OrderByItem($column);

        self::assertSame($column, $orderBy->expression());
        self::assertSame(SortDirection::ASC, $orderBy->direction());
        self::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
    }

    public function test_desc_creates_new_instance_with_descending_direction() : void
    {
        $column = Column::name('created_at');
        $orderBy = new OrderByItem($column);

        $descOrderBy = $orderBy->desc();

        self::assertNotSame($orderBy, $descOrderBy);
        self::assertSame(SortDirection::DESC, $descOrderBy->direction());
        self::assertSame($column, $descOrderBy->expression());
    }

    public function test_fluent_methods_return_new_instances() : void
    {
        $column = Column::name('score');
        $original = new OrderByItem($column);

        $modified = $original->desc()->nullsFirst();

        self::assertNotSame($original, $modified);
        self::assertSame(SortDirection::ASC, $original->direction());
        self::assertSame(NullsPosition::DEFAULT, $original->nulls());
        self::assertSame(SortDirection::DESC, $modified->direction());
        self::assertSame(NullsPosition::FIRST, $modified->nulls());
    }

    public function test_from_ast_with_ascending_direction() : void
    {
        $columnNode = Column::name('name')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_ASC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_DEFAULT,
        ]);

        $orderBy = OrderByItem::fromAst($sortBy);

        self::assertSame(SortDirection::ASC, $orderBy->direction());
        self::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
        self::assertInstanceOf(Column::class, $orderBy->expression());
    }

    public function test_from_ast_with_default_direction() : void
    {
        $columnNode = Column::name('id')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_DEFAULT,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_DEFAULT,
        ]);

        $orderBy = OrderByItem::fromAst($sortBy);

        self::assertSame(SortDirection::DEFAULT, $orderBy->direction());
        self::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
    }

    public function test_from_ast_with_descending_direction() : void
    {
        $columnNode = Column::name('created_at')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_DESC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_DEFAULT,
        ]);

        $orderBy = OrderByItem::fromAst($sortBy);

        self::assertSame(SortDirection::DESC, $orderBy->direction());
        self::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
    }

    public function test_from_ast_with_nulls_first() : void
    {
        $columnNode = Column::name('priority')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_ASC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_FIRST,
        ]);

        $orderBy = OrderByItem::fromAst($sortBy);

        self::assertSame(NullsPosition::FIRST, $orderBy->nulls());
    }

    public function test_from_ast_with_nulls_last() : void
    {
        $columnNode = Column::name('priority')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_DESC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_LAST,
        ]);

        $orderBy = OrderByItem::fromAst($sortBy);

        self::assertSame(NullsPosition::LAST, $orderBy->nulls());
    }

    public function test_nulls_first_creates_new_instance_with_first_nulls_position() : void
    {
        $column = Column::name('priority');
        $orderBy = new OrderByItem($column);

        $nullsFirstOrderBy = $orderBy->nullsFirst();

        self::assertNotSame($orderBy, $nullsFirstOrderBy);
        self::assertSame(NullsPosition::FIRST, $nullsFirstOrderBy->nulls());
        self::assertSame($column, $nullsFirstOrderBy->expression());
    }

    public function test_nulls_last_creates_new_instance_with_last_nulls_position() : void
    {
        $column = Column::name('priority');
        $orderBy = new OrderByItem($column);

        $nullsLastOrderBy = $orderBy->nullsLast();

        self::assertNotSame($orderBy, $nullsLastOrderBy);
        self::assertSame(NullsPosition::LAST, $nullsLastOrderBy->nulls());
        self::assertSame($column, $nullsLastOrderBy->expression());
    }

    public function test_round_trip_with_all_variations() : void
    {
        $testCases = [
            ['id', Column::name('id'), SortDirection::ASC, NullsPosition::DEFAULT],
            ['name', Column::name('name'), SortDirection::DESC, NullsPosition::FIRST],
            ['created_at', Column::name('created_at'), SortDirection::ASC, NullsPosition::LAST],
            ['email', Column::tableColumn('users', 'email'), SortDirection::DESC, NullsPosition::DEFAULT],
            ['score', Column::schemaTableColumn('public', 'users', 'score'), SortDirection::DEFAULT, NullsPosition::FIRST],
        ];

        foreach ($testCases as [$name, $expression, $direction, $nulls]) {
            $original = new OrderByItem($expression, $direction, $nulls);
            $ast = $original->toAst();
            $restored = OrderByItem::fromAst($ast);

            self::assertEquals($direction, $restored->direction(), "Direction mismatch for {$name}");
            self::assertEquals($nulls, $restored->nulls(), "Nulls position mismatch for {$name}");

            $restoredExpression = $restored->expression();
            self::assertInstanceOf(Column::class, $restoredExpression);
            self::assertEquals($expression->parts(), $restoredExpression->parts(), "Expression mismatch for {$name}");
        }
    }

    public function test_to_ast_creates_sort_by_node() : void
    {
        $column = Column::name('name');
        $orderBy = new OrderByItem($column);

        $ast = $orderBy->toAst();

        self::assertInstanceOf(SortBy::class, $ast);
        self::assertNotNull($ast->getNode());
        self::assertSame(SortByDir::SORTBY_ASC, $ast->getSortbyDir());
        self::assertSame(SortByNulls::SORTBY_NULLS_DEFAULT, $ast->getSortbyNulls());
    }

    public function test_to_ast_with_all_options() : void
    {
        $column = Column::name('priority');
        $orderBy = new OrderByItem($column, SortDirection::DESC, NullsPosition::LAST);

        $ast = $orderBy->toAst();

        self::assertInstanceOf(SortBy::class, $ast);
        self::assertSame(SortByDir::SORTBY_DESC, $ast->getSortbyDir());
        self::assertSame(SortByNulls::SORTBY_NULLS_LAST, $ast->getSortbyNulls());
    }

    public function test_to_ast_with_complex_column_reference() : void
    {
        $column = Column::tableColumn('users', 'email');
        $orderBy = new OrderByItem($column, SortDirection::ASC, NullsPosition::FIRST);

        $ast = $orderBy->toAst();

        self::assertInstanceOf(SortBy::class, $ast);

        $restored = OrderByItem::fromAst($ast);
        self::assertInstanceOf(Column::class, $restored->expression());
        self::assertSame(['users', 'email'], $restored->expression()->parts());
    }
}

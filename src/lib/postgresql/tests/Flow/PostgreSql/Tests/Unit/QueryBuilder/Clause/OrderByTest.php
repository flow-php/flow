<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\SortBy;
use Flow\PostgreSql\Protobuf\AST\SortByDir;
use Flow\PostgreSql\Protobuf\AST\SortByNulls;
use Flow\PostgreSql\QueryBuilder\Clause\NullsPosition;
use Flow\PostgreSql\QueryBuilder\Clause\OrderBy;
use Flow\PostgreSql\QueryBuilder\Clause\SortDirection;
use Flow\PostgreSql\QueryBuilder\Expression\Column;
use PHPUnit\Framework\TestCase;

final class OrderByTest extends TestCase
{
    public function test_asc_creates_new_instance_with_ascending_direction(): void
    {
        $column = Column::name('name');
        $orderBy = new OrderBy($column);

        $ascOrderBy = $orderBy->asc();

        static::assertNotSame($orderBy, $ascOrderBy);
        static::assertSame(SortDirection::ASC, $ascOrderBy->direction());
        static::assertSame($column, $ascOrderBy->expression());
    }

    public function test_constructor_with_all_parameters(): void
    {
        $column = Column::name('priority');
        $orderBy = new OrderBy($column, SortDirection::DESC, NullsPosition::FIRST);

        static::assertSame($column, $orderBy->expression());
        static::assertSame(SortDirection::DESC, $orderBy->direction());
        static::assertSame(NullsPosition::FIRST, $orderBy->nulls());
    }

    public function test_constructor_with_default_parameters(): void
    {
        $column = Column::name('name');
        $orderBy = new OrderBy($column);

        static::assertSame($column, $orderBy->expression());
        static::assertSame(SortDirection::ASC, $orderBy->direction());
        static::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
    }

    public function test_desc_creates_new_instance_with_descending_direction(): void
    {
        $column = Column::name('created_at');
        $orderBy = new OrderBy($column);

        $descOrderBy = $orderBy->desc();

        static::assertNotSame($orderBy, $descOrderBy);
        static::assertSame(SortDirection::DESC, $descOrderBy->direction());
        static::assertSame($column, $descOrderBy->expression());
    }

    public function test_fluent_methods_return_new_instances(): void
    {
        $column = Column::name('score');
        $original = new OrderBy($column);

        $modified = $original->desc()->nullsFirst();

        static::assertNotSame($original, $modified);
        static::assertSame(SortDirection::ASC, $original->direction());
        static::assertSame(NullsPosition::DEFAULT, $original->nulls());
        static::assertSame(SortDirection::DESC, $modified->direction());
        static::assertSame(NullsPosition::FIRST, $modified->nulls());
    }

    public function test_from_ast_with_ascending_direction(): void
    {
        $columnNode = Column::name('name')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_ASC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_DEFAULT,
        ]);

        $orderBy = OrderBy::fromAst($sortBy);

        static::assertSame(SortDirection::ASC, $orderBy->direction());
        static::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
        static::assertInstanceOf(Column::class, $orderBy->expression());
    }

    public function test_from_ast_with_default_direction(): void
    {
        $columnNode = Column::name('id')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_DEFAULT,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_DEFAULT,
        ]);

        $orderBy = OrderBy::fromAst($sortBy);

        static::assertSame(SortDirection::DEFAULT, $orderBy->direction());
        static::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
    }

    public function test_from_ast_with_descending_direction(): void
    {
        $columnNode = Column::name('created_at')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_DESC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_DEFAULT,
        ]);

        $orderBy = OrderBy::fromAst($sortBy);

        static::assertSame(SortDirection::DESC, $orderBy->direction());
        static::assertSame(NullsPosition::DEFAULT, $orderBy->nulls());
    }

    public function test_from_ast_with_nulls_first(): void
    {
        $columnNode = Column::name('priority')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_ASC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_FIRST,
        ]);

        $orderBy = OrderBy::fromAst($sortBy);

        static::assertSame(NullsPosition::FIRST, $orderBy->nulls());
    }

    public function test_from_ast_with_nulls_last(): void
    {
        $columnNode = Column::name('priority')->toAst();

        $sortBy = new SortBy([
            'node' => $columnNode,
            'sortby_dir' => SortByDir::SORTBY_DESC,
            'sortby_nulls' => SortByNulls::SORTBY_NULLS_LAST,
        ]);

        $orderBy = OrderBy::fromAst($sortBy);

        static::assertSame(NullsPosition::LAST, $orderBy->nulls());
    }

    public function test_nulls_first_creates_new_instance_with_first_nulls_position(): void
    {
        $column = Column::name('priority');
        $orderBy = new OrderBy($column);

        $nullsFirstOrderBy = $orderBy->nullsFirst();

        static::assertNotSame($orderBy, $nullsFirstOrderBy);
        static::assertSame(NullsPosition::FIRST, $nullsFirstOrderBy->nulls());
        static::assertSame($column, $nullsFirstOrderBy->expression());
    }

    public function test_nulls_last_creates_new_instance_with_last_nulls_position(): void
    {
        $column = Column::name('priority');
        $orderBy = new OrderBy($column);

        $nullsLastOrderBy = $orderBy->nullsLast();

        static::assertNotSame($orderBy, $nullsLastOrderBy);
        static::assertSame(NullsPosition::LAST, $nullsLastOrderBy->nulls());
        static::assertSame($column, $nullsLastOrderBy->expression());
    }

    public function test_round_trip_with_all_variations(): void
    {
        $testCases = [
            ['id', Column::name('id'), SortDirection::ASC, NullsPosition::DEFAULT],
            ['name', Column::name('name'), SortDirection::DESC, NullsPosition::FIRST],
            ['created_at', Column::name('created_at'), SortDirection::ASC, NullsPosition::LAST],
            ['email', Column::tableColumn('users', 'email'), SortDirection::DESC, NullsPosition::DEFAULT],
            [
                'score',
                Column::schemaTableColumn('public', 'users', 'score'),
                SortDirection::DEFAULT,
                NullsPosition::FIRST,
            ],
        ];

        foreach ($testCases as [$name, $expression, $direction, $nulls]) {
            $original = new OrderBy($expression, $direction, $nulls);
            $ast = $original->toAst();
            $restored = OrderBy::fromAst($ast);

            static::assertEquals($direction, $restored->direction(), "Direction mismatch for {$name}");
            static::assertEquals($nulls, $restored->nulls(), "Nulls position mismatch for {$name}");

            $restoredExpression = $restored->expression();
            static::assertInstanceOf(Column::class, $restoredExpression);
            static::assertEquals($expression->parts(), $restoredExpression->parts(), "Expression mismatch for {$name}");
        }
    }

    public function test_to_ast_creates_sort_by_node(): void
    {
        $column = Column::name('name');
        $orderBy = new OrderBy($column);

        $ast = $orderBy->toAst();

        static::assertInstanceOf(SortBy::class, $ast);
        static::assertNotNull($ast->getNode());
        static::assertSame(SortByDir::SORTBY_ASC, $ast->getSortbyDir());
        static::assertSame(SortByNulls::SORTBY_NULLS_DEFAULT, $ast->getSortbyNulls());
    }

    public function test_to_ast_with_all_options(): void
    {
        $column = Column::name('priority');
        $orderBy = new OrderBy($column, SortDirection::DESC, NullsPosition::LAST);

        $ast = $orderBy->toAst();

        static::assertInstanceOf(SortBy::class, $ast);
        static::assertSame(SortByDir::SORTBY_DESC, $ast->getSortbyDir());
        static::assertSame(SortByNulls::SORTBY_NULLS_LAST, $ast->getSortbyNulls());
    }

    public function test_to_ast_with_complex_column_reference(): void
    {
        $column = Column::tableColumn('users', 'email');
        $orderBy = new OrderBy($column, SortDirection::ASC, NullsPosition::FIRST);

        $ast = $orderBy->toAst();

        static::assertInstanceOf(SortBy::class, $ast);

        $restored = OrderBy::fromAst($ast);
        $expression = $restored->expression();
        static::assertInstanceOf(Column::class, $expression);
        static::assertSame(['users', 'email'], $expression->parts());
    }
}

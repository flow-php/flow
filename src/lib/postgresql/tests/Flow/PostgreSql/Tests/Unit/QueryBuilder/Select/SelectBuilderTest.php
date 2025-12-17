<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Select;

use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Clause\{OrderByItem, SortDirection};
use Flow\PostgreSql\QueryBuilder\Condition\{Comparison, ComparisonOperator};
use Flow\PostgreSql\QueryBuilder\Expression\{AggregateCall, Column, Literal, Star};
use Flow\PostgreSql\QueryBuilder\Select\SelectBuilder;
use Flow\PostgreSql\QueryBuilder\Table\Table;
use PHPUnit\Framework\TestCase;

final class SelectBuilderTest extends TestCase
{
    public function test_immutability_from() : void
    {
        $builder1 = SelectBuilder::create()->select(Column::name('id'));
        $builder2 = $builder1->from(new Table('users'));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_join() : void
    {
        $builder1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('users'));
        $builder2 = $builder1->leftJoin(
            new Table('orders'),
            new Comparison(Column::name('user_id'), ComparisonOperator::EQ, Column::name('id'))
        );

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_limit() : void
    {
        $builder1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('users'));
        $builder2 = $builder1->limit(10);

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_offset() : void
    {
        $builder1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('users'));
        $builder2 = $builder1->offset(20);

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_order_by() : void
    {
        $builder1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('users'));
        $builder2 = $builder1->orderBy(new OrderByItem(Column::name('id')));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_select() : void
    {
        $builder1 = SelectBuilder::create();
        $builder2 = $builder1->select(Column::name('id'));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_immutability_where() : void
    {
        $builder1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('users'));
        $builder2 = $builder1->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(true)));

        self::assertNotSame($builder1, $builder2);
    }

    public function test_round_trip_simple() : void
    {
        $original = SelectBuilder::create()
            ->select(Column::name('id'), Column::name('name'))
            ->from(new Table('users'))
            ->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(true)))
            ->orderBy(new OrderByItem(Column::name('name')))
            ->limit(10);

        $ast = $original->toAst();
        $restored = SelectBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        self::assertEquals($ast->serializeToString(), $restoredAst->serializeToString());
    }

    public function test_round_trip_with_aggregate() : void
    {
        $original = SelectBuilder::create()
            ->select(
                Column::name('category'),
                new AggregateCall(['count'], [], true),
                new AggregateCall(['sum'], [Column::name('price')])
            )
            ->from(new Table('products'))
            ->groupBy(Column::name('category'))
            ->having(new Comparison(new AggregateCall(['count'], [], true), ComparisonOperator::GT, Literal::int(5)))
            ->orderBy(new OrderByItem(Column::name('category')));

        $ast = $original->toAst();
        $restored = SelectBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        self::assertEquals($ast->serializeToString(), $restoredAst->serializeToString());
    }

    public function test_round_trip_with_joins() : void
    {
        $original = SelectBuilder::create()
            ->select(Column::tableColumn('u', 'name'), Column::tableColumn('o', 'total'))
            ->from((new Table('users'))->as('u'))
            ->leftJoin(
                (new Table('orders'))->as('o'),
                new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('o', 'user_id'))
            )
            ->where(new Comparison(Column::tableColumn('o', 'total'), ComparisonOperator::GT, Literal::int(100)))
            ->orderBy(new OrderByItem(Column::tableColumn('o', 'total'), SortDirection::DESC));

        $ast = $original->toAst();
        $restored = SelectBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        self::assertEquals($ast->serializeToString(), $restoredAst->serializeToString());
    }

    public function test_round_trip_with_set_operation() : void
    {
        $query1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table1'))
            ->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(true)));

        $query2 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table2'));

        $original = $query1->union($query2)
            ->orderBy(new OrderByItem(Column::name('id')))
            ->limit(20);

        $ast = $original->toAst();
        $restored = SelectBuilder::fromAst($ast);
        $restoredAst = $restored->toAst();

        self::assertEquals($ast->serializeToString(), $restoredAst->serializeToString());
    }

    public function test_select_distinct() : void
    {
        $query = SelectBuilder::create()
            ->selectDistinct(Column::name('city'))
            ->from(new Table('users'));

        $ast = $query->toAst();
        $distinctClause = $ast->getDistinctClause();

        self::assertNotNull($distinctClause);
    }

    public function test_select_distinct_on() : void
    {
        $query = SelectBuilder::create()
            ->selectDistinctOn([Column::name('category')], Column::name('category'), Column::name('product_name'))
            ->from(new Table('products'));

        $ast = $query->toAst();
        $distinctClause = $ast->getDistinctClause();

        self::assertNotNull($distinctClause);
        self::assertCount(1, $distinctClause);
    }

    public function test_select_from_multiple_tables() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'), new Table('orders'));

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
        self::assertCount(2, $fromClause);
    }

    public function test_select_only_limit() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->limit(10);

        $ast = $query->toAst();
        $limitCount = $ast->getLimitCount();
        $limitOffset = $ast->getLimitOffset();

        self::assertNotNull($limitCount);
        self::assertNull($limitOffset);
    }

    public function test_select_only_offset() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->offset(20);

        $ast = $query->toAst();
        $limitCount = $ast->getLimitCount();
        $limitOffset = $ast->getLimitOffset();

        self::assertNull($limitCount);
        self::assertNotNull($limitOffset);
    }

    public function test_select_star() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'));

        $ast = $query->toAst();
        $targetList = $ast->getTargetList();

        self::assertNotNull($targetList);
        self::assertCount(1, $targetList);
    }

    public function test_select_with_cross_join() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('t1'))
            ->crossJoin(new Table('t2'));

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
    }

    public function test_select_with_except() : void
    {
        $query1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table1'));

        $query2 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table2'));

        $exceptQuery = $query1->except($query2);

        $ast = $exceptQuery->toAst();

        self::assertNotNull($ast->getLarg());
        self::assertNotNull($ast->getRarg());
    }

    public function test_select_with_for_key_share() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->forKeyShare();

        $ast = $query->toAst();
        $lockingClause = $ast->getLockingClause();

        self::assertNotNull($lockingClause);
        self::assertCount(1, $lockingClause);
    }

    public function test_select_with_for_no_key_update() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->forNoKeyUpdate();

        $ast = $query->toAst();
        $lockingClause = $ast->getLockingClause();

        self::assertNotNull($lockingClause);
        self::assertCount(1, $lockingClause);
    }

    public function test_select_with_for_share() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->forShare();

        $ast = $query->toAst();
        $lockingClause = $ast->getLockingClause();

        self::assertNotNull($lockingClause);
        self::assertCount(1, $lockingClause);
    }

    public function test_select_with_for_update() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->forUpdate();

        $ast = $query->toAst();
        $lockingClause = $ast->getLockingClause();

        self::assertNotNull($lockingClause);
        self::assertCount(1, $lockingClause);
    }

    public function test_select_with_full_join() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from((new Table('users'))->as('u'))
            ->fullJoin(
                (new Table('orders'))->as('o'),
                new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('o', 'user_id'))
            );

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
    }

    public function test_select_with_group_by_having() : void
    {
        $query = SelectBuilder::create()
            ->select(Column::name('category'), new AggregateCall(['count'], [], true))
            ->from(new Table('products'))
            ->groupBy(Column::name('category'))
            ->having(new Comparison(new AggregateCall(['count'], [], true), ComparisonOperator::GT, Literal::int(5)));

        $ast = $query->toAst();
        $groupClause = $ast->getGroupClause();
        $havingClause = $ast->getHavingClause();

        self::assertNotNull($groupClause);
        self::assertCount(1, $groupClause);
        self::assertNotNull($havingClause);
    }

    public function test_select_with_inner_join() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from((new Table('users'))->as('u'))
            ->join(
                (new Table('orders'))->as('o'),
                new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('o', 'user_id'))
            );

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
    }

    public function test_select_with_intersect() : void
    {
        $query1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table1'));

        $query2 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table2'));

        $intersectQuery = $query1->intersect($query2);

        $ast = $intersectQuery->toAst();

        self::assertNotNull($ast->getLarg());
        self::assertNotNull($ast->getRarg());
    }

    public function test_select_with_join() : void
    {
        $query = SelectBuilder::create()
            ->select(Column::tableColumn('u', 'name'), Column::tableColumn('o', 'total'))
            ->from((new Table('users'))->as('u'))
            ->leftJoin(
                (new Table('orders'))->as('o'),
                new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('o', 'user_id'))
            );

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
        self::assertCount(1, $fromClause);
    }

    public function test_select_with_multiple_joins() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from((new Table('users'))->as('u'))
            ->leftJoin(
                (new Table('orders'))->as('o'),
                new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('o', 'user_id'))
            )
            ->leftJoin(
                (new Table('payments'))->as('p'),
                new Comparison(Column::tableColumn('o', 'id'), ComparisonOperator::EQ, Column::tableColumn('p', 'order_id'))
            );

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
    }

    public function test_select_with_order_by_multiple_columns() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->orderBy(
                new OrderByItem(Column::name('last_name')),
                new OrderByItem(Column::name('first_name'), SortDirection::DESC)
            );

        $ast = $query->toAst();
        $sortClause = $ast->getSortClause();

        self::assertNotNull($sortClause);
        self::assertCount(2, $sortClause);
    }

    public function test_select_with_order_limit_offset() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('events'))
            ->orderBy(new OrderByItem(Column::name('created_at'), SortDirection::DESC))
            ->limit(10)
            ->offset(20);

        $ast = $query->toAst();
        $sortClause = $ast->getSortClause();
        $limitCount = $ast->getLimitCount();
        $limitOffset = $ast->getLimitOffset();

        self::assertNotNull($sortClause);
        self::assertCount(1, $sortClause);
        self::assertNotNull($limitCount);
        self::assertNotNull($limitOffset);
    }

    public function test_select_with_right_join() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from((new Table('users'))->as('u'))
            ->rightJoin(
                (new Table('orders'))->as('o'),
                new Comparison(Column::tableColumn('u', 'id'), ComparisonOperator::EQ, Column::tableColumn('o', 'user_id'))
            );

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
    }

    public function test_select_with_schema_qualified_table() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users', 'public'));

        $ast = $query->toAst();
        $fromClause = $ast->getFromClause();

        self::assertNotNull($fromClause);
        self::assertCount(1, $fromClause);

        $rangeVar = $fromClause[0]->getRangeVar();
        self::assertNotNull($rangeVar);
        self::assertSame('public', $rangeVar->getSchemaname());
    }

    public function test_select_with_union() : void
    {
        $query1 = SelectBuilder::create()
            ->select(Column::name('name'))
            ->from(new Table('users'));

        $query2 = SelectBuilder::create()
            ->select(Column::name('name'))
            ->from(new Table('admins'));

        $unionQuery = $query1->union($query2);

        $ast = $unionQuery->toAst();

        self::assertNotNull($ast->getLarg());
        self::assertNotNull($ast->getRarg());
    }

    public function test_select_with_union_all() : void
    {
        $query1 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table1'));

        $query2 = SelectBuilder::create()
            ->select(Column::name('id'))
            ->from(new Table('table2'));

        $unionQuery = $query1->unionAll($query2);

        $ast = $unionQuery->toAst();

        self::assertTrue($ast->getAll());
    }

    public function test_select_with_where() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->where(new Comparison(Column::name('active'), ComparisonOperator::EQ, Literal::bool(true)));

        $ast = $query->toAst();
        $whereClause = $ast->getWhereClause();

        self::assertNotNull($whereClause);
    }

    public function test_simple_select() : void
    {
        $query = SelectBuilder::create()
            ->select(Column::name('id'), Column::name('name'))
            ->from(new Table('users'));

        $ast = $query->toAst();
        self::assertInstanceOf(SelectStmt::class, $ast);

        $targetList = $ast->getTargetList();
        self::assertNotNull($targetList);
        self::assertCount(2, $targetList);

        $fromClause = $ast->getFromClause();
        self::assertNotNull($fromClause);
        self::assertCount(1, $fromClause);
    }
}

<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Select;

use function Flow\PostgreSql\DSL\{
    agg_avg,
    agg_count,
    agg_max,
    agg_min,
    agg_sum,
    all_,
    and_,
    any_,
    array_expr,
    asc,
    between,
    binary_expr,
    case_when,
    cast,
    coalesce,
    col,
    column_type_integer,
    column_type_text,
    cte,
    current_date,
    current_time,
    current_timestamp,
    derived,
    desc,
    distinct_from,
    eq,
    exists,
    func,
    ge,
    greatest,
    gt,
    in_,
    is_null,
    is_true,
    lateral,
    le,
    least,
    like,
    literal,
    lt,
    ne,
    not_,
    nullif,
    or_,
    order_by,
    param,
    row_expr,
    select,
    similar_to,
    star,
    sub_select,
    table,
    table_func,
    when,
    window_def,
    window_func,
    with
};

use Flow\PostgreSql\Protobuf\AST\SelectStmt;
use Flow\PostgreSql\QueryBuilder\Clause\{CTEMaterialization, NullsPosition, OrderBy, SortDirection};
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
        $builder2 = $builder1->orderBy(new OrderBy(Column::name('id')));

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
            ->orderBy(new OrderBy(Column::name('name')))
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
            ->orderBy(new OrderBy(Column::name('category')));

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
            ->orderBy(new OrderBy(Column::tableColumn('o', 'total'), SortDirection::DESC));

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
            ->orderBy(new OrderBy(Column::name('id')))
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

    public function test_select_except_all_to_sql() : void
    {
        self::assertSame(
            'SELECT id FROM all_users EXCEPT ALL SELECT id FROM banned_users',
            select()->select(col('id'))->from(table('all_users'))
                ->exceptAll(select()->select(col('id'))->from(table('banned_users')))
                ->toSql()
        );
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

    public function test_select_intersect_all_to_sql() : void
    {
        self::assertSame(
            'SELECT id FROM users INTERSECT ALL SELECT id FROM admins',
            select()->select(col('id'))->from(table('users'))
                ->intersectAll(select()->select(col('id'))->from(table('admins')))
                ->toSql()
        );
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

    public function test_select_with_aggregates_to_sql() : void
    {
        self::assertSame(
            'SELECT category, count(*), sum(amount), avg(price), min(created_at), max(updated_at) FROM orders GROUP BY category',
            select()
                ->select(
                    col('category'),
                    agg_count(),
                    agg_sum(col('amount')),
                    agg_avg(col('price')),
                    agg_min(col('created_at')),
                    agg_max(col('updated_at'))
                )
                ->from(table('orders'))
                ->groupBy(col('category'))
                ->toSql()
        );
    }

    public function test_select_with_aliased_columns_to_sql() : void
    {
        self::assertSame(
            'SELECT first_name AS fname, last_name AS lname FROM users',
            select()
                ->select(col('first_name')->as('fname'), col('last_name')->as('lname'))
                ->from(table('users'))
                ->toSql()
        );
    }

    public function test_select_with_all_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM products WHERE price > ALL (SELECT price FROM budget_products)',
            select()
                ->select(star())
                ->from(table('products'))
                ->where(all_(col('price'), ComparisonOperator::GT, select()->select(col('price'))->from(table('budget_products'))))
                ->toSql()
        );
    }

    public function test_select_with_any_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM products WHERE price > ANY (SELECT price FROM discounted_products)',
            select()
                ->select(star())
                ->from(table('products'))
                ->where(any_(col('price'), ComparisonOperator::GT, select()->select(col('price'))->from(table('discounted_products'))))
                ->toSql()
        );
    }

    public function test_select_with_array_constructor_to_sql() : void
    {
        self::assertSame(
            'SELECT ARRAY[1, 2, 3] AS numbers FROM users',
            select()
                ->select(array_expr([literal(1), literal(2), literal(3)])->as('numbers'))
                ->from(table('users'))
                ->toSql()
        );
    }

    public function test_select_with_array_expression_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users WHERE id IN (1, 2, 3)',
            select()
                ->select(star())
                ->from(table('users'))
                ->where(in_(col('id'), [literal(1), literal(2), literal(3)]))
                ->toSql()
        );
    }

    public function test_select_with_between_condition_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM products WHERE price BETWEEN 10 AND 100',
            select()
                ->select(star())
                ->from(table('products'))
                ->where(between(col('price'), literal(10), literal(100)))
                ->toSql()
        );
    }

    public function test_select_with_case_expression_to_sql() : void
    {
        self::assertSame(
            "SELECT name, CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END AS price_category FROM products",
            select()
                ->select(
                    col('name'),
                    case_when([
                        when(binary_expr(col('price'), '>', literal(100)), literal('expensive')),
                        when(binary_expr(col('price'), '>', literal(50)), literal('moderate')),
                    ], literal('cheap'))->as('price_category')
                )
                ->from(table('products'))
                ->toSql()
        );
    }

    public function test_select_with_coalesce_to_sql() : void
    {
        self::assertSame(
            "SELECT id, COALESCE(nickname, name, 'Anonymous') AS display_name FROM users",
            select()
                ->select(
                    col('id'),
                    coalesce(col('nickname'), col('name'), literal('Anonymous'))->as('display_name')
                )
                ->from(table('users'))
                ->toSql()
        );
    }

    public function test_select_with_complex_and_or_conditions_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE status = 'active' AND (age >= 18 OR guardian_approved = true)",
            select()
                ->select(star())
                ->from(table('users'))
                ->where(
                    and_(
                        eq(col('status'), literal('active')),
                        or_(
                            ge(col('age'), literal(18)),
                            eq(col('guardian_approved'), literal(true))
                        )
                    )
                )
                ->toSql()
        );
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

    public function test_select_with_cte_materialized_to_sql() : void
    {
        self::assertSame(
            'WITH active_users AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users',
            with(cte('active_users', select()->select(col('id'), col('name'))->from(table('users'))->where(eq(col('active'), literal(true))), [], CTEMaterialization::MATERIALIZED))
                ->select(star())
                ->from(table('active_users'))
                ->toSql()
        );
    }

    public function test_select_with_cte_not_materialized_to_sql() : void
    {
        self::assertSame(
            'WITH active_users AS NOT MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users',
            with(cte('active_users', select()->select(col('id'), col('name'))->from(table('users'))->where(eq(col('active'), literal(true))), [], CTEMaterialization::NOT_MATERIALIZED))
                ->select(star())
                ->from(table('active_users'))
                ->toSql()
        );
    }

    public function test_select_with_cte_to_sql() : void
    {
        self::assertSame(
            'WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users',
            with(
                cte('active_users', select()->select(col('id'), col('name'))->from(table('users'))->where(eq(col('active'), literal(true))))
            )
                ->select(star())
                ->from(table('active_users'))
                ->toSql()
        );
    }

    public function test_select_with_derived_table_join_to_sql() : void
    {
        self::assertSame(
            'SELECT users.name, order_stats.order_count FROM users JOIN (SELECT user_id, count(*) AS order_count FROM orders GROUP BY user_id) order_stats ON users.id = order_stats.user_id',
            select()
                ->select(col('users.name'), col('order_stats.order_count'))
                ->from(table('users'))
                ->join(
                    derived(
                        select()->select(col('user_id'), agg_count()->as('order_count'))->from(table('orders'))->groupBy(col('user_id')),
                        'order_stats'
                    ),
                    eq(col('users.id'), col('order_stats.user_id'))
                )
                ->toSql()
        );
    }

    public function test_select_with_derived_table_to_sql() : void
    {
        self::assertSame(
            'SELECT u.name, o.total FROM users u LEFT JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id',
            select()
                ->select(col('u.name'), col('o.total'))
                ->from(table('users')->as('u'))
                ->leftJoin(
                    derived(
                        select()->select(col('user_id'), agg_sum(col('amount'))->as('total'))->from(table('orders'))->groupBy(col('user_id')),
                        'o'
                    ),
                    eq(col('u.id'), col('o.user_id'))
                )
                ->toSql()
        );
    }

    public function test_select_with_distinct_count_to_sql() : void
    {
        self::assertSame(
            'SELECT count(DISTINCT user_id) AS unique_users FROM orders',
            select()->select(agg_count(col('user_id'), true)->as('unique_users'))->from(table('orders'))->toSql()
        );
    }

    public function test_select_with_distinct_to_sql() : void
    {
        self::assertSame(
            'SELECT DISTINCT city FROM users',
            select()->selectDistinct(col('city'))->from(table('users'))->toSql()
        );
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

    public function test_select_with_exists_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)',
            select()
                ->select(star())
                ->from(table('users'))
                ->where(exists(
                    select()->select(literal(1))->from(table('orders'))->where(eq(col('orders.user_id'), col('users.id')))
                ))
                ->toSql()
        );
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

    public function test_select_with_for_update_skip_locked() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->forUpdateSkipLocked();

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

    public function test_select_with_function_call_to_sql() : void
    {
        self::assertSame(
            'SELECT name, upper(name) AS upper_name, length(name) AS name_length FROM users',
            select()
                ->select(
                    col('name'),
                    func('upper', [col('name')])->as('upper_name'),
                    func('length', [col('name')])->as('name_length')
                )
                ->from(table('users'))
                ->toSql()
        );
    }

    public function test_select_with_greatest_to_sql() : void
    {
        self::assertSame(
            'SELECT id, GREATEST(a, b, c) AS max_value FROM numbers',
            select()
                ->select(col('id'), greatest(col('a'), col('b'), col('c'))->as('max_value'))
                ->from(table('numbers'))
                ->toSql()
        );
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

    public function test_select_with_in_condition_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')",
            select()
                ->select(star())
                ->from(table('users'))
                ->where(in_(col('status'), [literal('active'), literal('pending'), literal('verified')]))
                ->toSql()
        );
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

    public function test_select_with_is_distinct_from_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users WHERE status IS DISTINCT FROM NULL',
            select()->select(star())->from(table('users'))->where(distinct_from(col('status'), literal(null)))->toSql()
        );
    }

    public function test_select_with_is_not_distinct_from_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE status IS NOT DISTINCT FROM 'active'",
            select()->select(star())->from(table('users'))->where(distinct_from(col('status'), literal('active'), true))->toSql()
        );
    }

    public function test_select_with_is_null_condition_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users WHERE deleted_at IS NULL',
            select()->select(star())->from(table('users'))->where(is_null(col('deleted_at')))->toSql()
        );
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

    public function test_select_with_lateral_join_to_sql() : void
    {
        self::assertSame(
            'SELECT u.name, recent.id FROM users u LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = u.id ORDER BY created_at DESC LIMIT 3) recent ON true',
            select()
                ->select(col('u.name'), col('recent.id'))
                ->from(table('users')->as('u'))
                ->leftJoin(
                    lateral(derived(
                        select()->select(star())->from(table('orders'))->where(eq(col('orders.user_id'), col('u.id')))->orderBy(desc(col('created_at')))->limit(3),
                        'recent'
                    )),
                    is_true(literal(true))
                )
                ->toSql()
        );
    }

    public function test_select_with_lateral_subquery_to_sql() : void
    {
        self::assertSame(
            'SELECT users.name, recent_orders.order_id FROM users CROSS JOIN LATERAL (SELECT order_id FROM orders WHERE orders.user_id = users.id ORDER BY created_at DESC LIMIT 5) recent_orders',
            select()
                ->select(col('users.name'), col('recent_orders.order_id'))
                ->from(table('users'))
                ->crossJoin(lateral(derived(
                    select()->select(col('order_id'))->from(table('orders'))->where(eq(col('orders.user_id'), col('users.id')))->orderBy(desc(col('created_at')))->limit(5),
                    'recent_orders'
                )))
                ->toSql()
        );
    }

    public function test_select_with_least_to_sql() : void
    {
        self::assertSame(
            'SELECT id, LEAST(a, b, c) AS min_value FROM numbers',
            select()
                ->select(col('id'), least(col('a'), col('b'), col('c'))->as('min_value'))
                ->from(table('numbers'))
                ->toSql()
        );
    }

    public function test_select_with_left_join_to_sql() : void
    {
        self::assertSame(
            'SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id',
            select()
                ->select(col('u.name'), col('o.total'))
                ->from(table('users')->as('u'))
                ->leftJoin(table('orders')->as('o'), eq(col('u.id'), col('o.user_id')))
                ->toSql()
        );
    }

    public function test_select_with_like_condition_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE email LIKE '%@example.com'",
            select()->select(star())->from(table('users'))->where(like(col('email'), literal('%@example.com')))->toSql()
        );
    }

    public function test_select_with_limit_offset_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20',
            select()->select(star())->from(table('users'))->orderBy(asc(col('id')))->limit(10)->offset(20)->toSql()
        );
    }

    public function test_select_with_multiple_conditions_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE active = true AND age >= 18 AND status <> 'banned'",
            select()
                ->select(star())
                ->from(table('users'))
                ->where(and_(
                    eq(col('active'), literal(true)),
                    ge(col('age'), literal(18)),
                    ne(col('status'), literal('banned'))
                ))
                ->toSql()
        );
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

    public function test_select_with_not_condition_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE NOT status = 'banned'",
            select()->select(star())->from(table('users'))->where(not_(eq(col('status'), literal('banned'))))->toSql()
        );
    }

    public function test_select_with_nullif_to_sql() : void
    {
        self::assertSame(
            'SELECT id, NULLIF(value, 0) AS safe_value FROM data',
            select()
                ->select(col('id'), nullif(col('value'), literal(0))->as('safe_value'))
                ->from(table('data'))
                ->toSql()
        );
    }

    public function test_select_with_order_by_asc_desc_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users ORDER BY last_name ASC, first_name DESC',
            select()->select(star())->from(table('users'))->orderBy(asc(col('last_name')), desc(col('first_name')))->toSql()
        );
    }

    public function test_select_with_order_by_multiple_columns() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('users'))
            ->orderBy(
                new OrderBy(Column::name('last_name')),
                new OrderBy(Column::name('first_name'), SortDirection::DESC)
            );

        $ast = $query->toAst();
        $sortClause = $ast->getSortClause();

        self::assertNotNull($sortClause);
        self::assertCount(2, $sortClause);
    }

    public function test_select_with_order_by_nulls_first_last_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM products ORDER BY price ASC NULLS FIRST, name DESC NULLS LAST',
            select()
                ->select(star())
                ->from(table('products'))
                ->orderBy(
                    order_by(col('price'), SortDirection::ASC, NullsPosition::FIRST),
                    order_by(col('name'), SortDirection::DESC, NullsPosition::LAST)
                )
                ->toSql()
        );
    }

    public function test_select_with_order_limit_offset() : void
    {
        $query = SelectBuilder::create()
            ->select(Star::all())
            ->from(new Table('events'))
            ->orderBy(new OrderBy(Column::name('created_at'), SortDirection::DESC))
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

    public function test_select_with_parameter_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users WHERE id = $1',
            select()->select(star())->from(table('users'))->where(eq(col('id'), param(1)))->toSql()
        );
    }

    public function test_select_with_raw_expression_in_select_to_sql() : void
    {
        self::assertSame(
            'SELECT id, 1 + 1 AS two FROM users',
            select()->select(col('id'), binary_expr(literal(1), '+', literal(1))->as('two'))->from(table('users'))->toSql()
        );
    }

    public function test_select_with_raw_expression_to_sql() : void
    {
        self::assertSame(
            'SELECT *, 1 + 1 AS calculated FROM users',
            select()
                ->select(star(), binary_expr(literal(1), '+', literal(1))->as('calculated'))
                ->from(table('users'))
                ->toSql()
        );
    }

    public function test_select_with_recursive_cte_window_functions_and_case_expression_to_sql() : void
    {
        $orgTreeAnchor = select()
            ->select(
                col('id'),
                col('name'),
                col('manager_id'),
                literal(0)->as('level'),
                cast(col('name'), column_type_text())->as('path')
            )
            ->from(table('employees'))
            ->where(is_null(col('manager_id')));

        $orgTreeRecursive = select()
            ->select(
                col('e.id'),
                col('e.name'),
                col('e.manager_id'),
                binary_expr(col('org_tree.level'), '+', literal(1)),
                binary_expr(
                    binary_expr(col('org_tree.path'), '||', literal(' -> ')),
                    '||',
                    col('e.name')
                )
            )
            ->from(table('employees')->as('e'))
            ->join(table('org_tree'), eq(col('e.manager_id'), col('org_tree.id')));

        $orgTreeQuery = $orgTreeAnchor->unionAll($orgTreeRecursive);

        $deptStats = select()
            ->select(
                col('department_id'),
                agg_avg(col('salary')),
                agg_max(col('salary')),
                agg_count()
            )
            ->from(table('employees'))
            ->groupBy(col('department_id'));

        $rankedEmployees = select()
            ->select(
                col('e.id'),
                col('e.name'),
                col('e.department_id'),
                col('e.salary'),
                window_func('row_number', [], [col('e.department_id')], [desc(col('e.salary'))]),
                func('round', [
                    binary_expr(
                        binary_expr(col('e.salary'), '/', col('ds.max_salary')),
                        '*',
                        literal(100)
                    ),
                    literal(2),
                ])
            )
            ->from(table('employees')->as('e'))
            ->join(table('dept_stats')->as('ds'), eq(col('e.department_id'), col('ds.department_id')));

        $query = with(
            cte('org_tree', $orgTreeQuery, ['id', 'name', 'manager_id', 'level', 'path']),
            cte('dept_stats', $deptStats, ['department_id', 'avg_salary', 'max_salary', 'employee_count']),
            cte('ranked_employees', $rankedEmployees, ['id', 'name', 'department_id', 'salary', 'salary_rank', 'pct_of_max'])
        )
            ->recursive()
            ->select(
                col('org_tree.name')->as('employee'),
                col('org_tree.level'),
                col('org_tree.path')->as('reporting_chain'),
                col('d.name')->as('department'),
                col('re.salary'),
                col('re.salary_rank'),
                col('re.pct_of_max'),
                col('ds.avg_salary')->as('dept_avg'),
                case_when([
                    when(binary_expr(col('re.salary'), '>', col('ds.avg_salary')), literal('Above Average')),
                ], literal('At/Below Average'))->as('salary_status')
            )
            ->from(table('org_tree'))
            ->join(table('ranked_employees')->as('re'), eq(col('org_tree.id'), col('re.id')))
            ->join(table('dept_stats')->as('ds'), eq(col('re.department_id'), col('ds.department_id')))
            ->join(table('departments')->as('d'), eq(col('re.department_id'), col('d.id')))
            ->where(and_(
                le(col('org_tree.level'), literal(3)),
                le(col('re.salary_rank'), literal(5))
            ))
            ->orderBy(
                asc(col('org_tree.level')),
                asc(col('d.name')),
                desc(col('re.salary'))
            );

        $expectedSql = <<<'SQL'
            WITH RECURSIVE org_tree(id, name, manager_id, level, path) AS (SELECT id, name, manager_id, 0 AS level, name::pg_catalog.text AS path FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, org_tree.level + 1, (org_tree.path || ' -> ') || e.name FROM employees e JOIN org_tree ON e.manager_id = org_tree.id), dept_stats(department_id, avg_salary, max_salary, employee_count) AS (SELECT department_id, avg(salary), max(salary), count(*) FROM employees GROUP BY department_id), ranked_employees(id, name, department_id, salary, salary_rank, pct_of_max) AS (SELECT e.id, e.name, e.department_id, e.salary, row_number() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC), round((e.salary / ds.max_salary) * 100, 2) FROM employees e JOIN dept_stats ds ON e.department_id = ds.department_id) SELECT org_tree.name AS employee, org_tree.level, org_tree.path AS reporting_chain, d.name AS department, re.salary, re.salary_rank, re.pct_of_max, ds.avg_salary AS dept_avg, CASE WHEN re.salary > ds.avg_salary THEN 'Above Average' ELSE 'At/Below Average' END AS salary_status FROM org_tree JOIN ranked_employees re ON org_tree.id = re.id JOIN dept_stats ds ON re.department_id = ds.department_id JOIN departments d ON re.department_id = d.id WHERE org_tree.level <= 3 AND re.salary_rank <= 5 ORDER BY org_tree.level ASC, d.name ASC, re.salary DESC
            SQL;

        self::assertSame($expectedSql, $query->toSql());
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

    public function test_select_with_row_expression_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE (first_name, last_name) = ('John', 'Doe')",
            select()
                ->select(star())
                ->from(table('users'))
                ->where(eq(
                    row_expr([col('first_name'), col('last_name')]),
                    row_expr([literal('John'), literal('Doe')])
                ))
                ->toSql()
        );
    }

    public function test_select_with_scalar_subquery_to_sql() : void
    {
        self::assertSame(
            'SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users',
            select()
                ->select(
                    col('name'),
                    sub_select(
                        select()->select(agg_count())->from(table('orders'))->where(eq(col('orders.user_id'), col('users.id')))
                    )->as('order_count')
                )
                ->from(table('users'))
                ->toSql()
        );
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

    public function test_select_with_similar_to_to_sql() : void
    {
        self::assertSame(
            "SELECT * FROM users WHERE name SIMILAR TO '%John%'",
            select()->select(star())->from(table('users'))->where(similar_to(col('name'), literal('%John%')))->toSql()
        );
    }

    public function test_select_with_table_function_to_sql() : void
    {
        self::assertSame(
            'SELECT value FROM generate_series(1, 10) value',
            select()->select(col('value'))->from(table_func(func('generate_series', [literal(1), literal(10)]))->as('value'))->toSql()
        );
    }

    public function test_select_with_type_cast_to_sql() : void
    {
        self::assertSame(
            'SELECT id, price::int AS price_int FROM products',
            select()
                ->select(col('id'), cast(col('price'), column_type_integer())->as('price_int'))
                ->from(table('products'))
                ->toSql()
        );
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

    public function test_select_with_where_comparison_operators_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM products WHERE price > 10 AND price < 100 AND quantity >= 1 AND weight <= 50',
            select()
                ->select(star())
                ->from(table('products'))
                ->where(and_(
                    gt(col('price'), literal(10)),
                    lt(col('price'), literal(100)),
                    ge(col('quantity'), literal(1)),
                    le(col('weight'), literal(50))
                ))
                ->toSql()
        );
    }

    public function test_select_with_window_definition_to_sql() : void
    {
        self::assertSame(
            'SELECT department, salary, row_number() OVER (ORDER BY salary DESC) AS rank FROM employees WINDOW w AS (PARTITION BY department ORDER BY salary DESC)',
            select()
                ->select(
                    col('department'),
                    col('salary'),
                    window_func('row_number', [], [], [order_by(col('salary'), SortDirection::DESC)])->as('rank')
                )
                ->from(table('employees'))
                ->window(window_def('w', [col('department')], [order_by(col('salary'), SortDirection::DESC)]))
                ->toSql()
        );
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

    public function test_simple_select_star_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users',
            select()->select(star())->from(table('users'))->toSql()
        );
    }

    public function test_simple_select_with_where_to_sql() : void
    {
        self::assertSame(
            'SELECT * FROM users WHERE active = true',
            select()->select(star())->from(table('users'))->where(eq(col('active'), literal(true)))->toSql()
        );
    }

    public function test_sql_value_function_current_date_to_sql() : void
    {
        self::assertSame(
            'SELECT current_date AS today',
            select()->select(current_date()->as('today'))->toSql()
        );
    }

    public function test_sql_value_function_current_time_to_sql() : void
    {
        self::assertSame(
            'SELECT current_time AS now_time',
            select()->select(current_time()->as('now_time'))->toSql()
        );
    }

    public function test_sql_value_function_current_timestamp_to_sql() : void
    {
        self::assertSame(
            'SELECT current_timestamp AS now',
            select()->select(current_timestamp()->as('now'))->toSql()
        );
    }

    public function test_sql_value_functions_with_other_columns_to_sql() : void
    {
        self::assertSame(
            'SELECT id, name, current_timestamp AS created_at FROM users',
            select()
                ->select(col('id'), col('name'), current_timestamp()->as('created_at'))
                ->from(table('users'))
                ->toSql()
        );
    }
}

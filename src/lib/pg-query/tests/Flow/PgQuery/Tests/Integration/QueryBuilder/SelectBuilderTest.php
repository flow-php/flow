<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    agg_avg,
    agg_count,
    agg_max,
    agg_min,
    agg_sum,
    all_sub_selects,
    any_sub_select,
    array_expr,
    asc,
    between,
    binary_expr,
    case_when,
    cast,
    coalesce,
    col_from_string,
    cond_and,
    cond_not,
    cond_or,
    cte,
    cte_ref,
    derived,
    desc,
    eq,
    exists,
    func,
    greatest,
    gt,
    gte,
    is_distinct_from,
    is_in,
    lateral,
    least,
    like,
    literal_bool,
    literal_int,
    literal_null,
    literal_string,
    lt,
    lte,
    neq,
    nullif,
    order_by,
    param,
    raw_cond,
    raw_expr,
    row_expr,
    select,
    select_with,
    similar_to,
    star,
    sub_select,
    table,
    table_func,
    when,
    window_def,
    window_func,
    with_cte
};
use Flow\PgQuery\QueryBuilder\Clause\{CTEMaterialization, NullsPosition, SortDirection};

use Flow\PgQuery\QueryBuilder\Condition\ComparisonOperator;

final class SelectBuilderTest extends PGQueryTestCase
{
    public function test_select_distinct_on() : void
    {
        $query = select()
            ->selectDistinctOn(
                [col_from_string('department')],
                col_from_string('name'),
                col_from_string('salary')
            )
            ->from(table('employees'))
            ->orderBy(asc(col_from_string('department')), desc(col_from_string('salary')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT DISTINCT ON (department) name, salary FROM employees ORDER BY department ASC, salary DESC'
        );
    }

    public function test_select_except_all() : void
    {
        $query1 = select()
            ->select(col_from_string('id'))
            ->from(table('all_users'));

        $query2 = select()
            ->select(col_from_string('id'))
            ->from(table('banned_users'));

        $query = $query1->exceptAll($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM all_users EXCEPT ALL SELECT id FROM banned_users'
        );
    }

    public function test_select_intersect_all() : void
    {
        $query1 = select()
            ->select(col_from_string('id'))
            ->from(table('users'));

        $query2 = select()
            ->select(col_from_string('id'))
            ->from(table('admins'));

        $query = $query1->intersectAll($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM users INTERSECT ALL SELECT id FROM admins'
        );
    }

    public function test_select_with_aggregates() : void
    {
        $query = select()
            ->select(
                col_from_string('category'),
                agg_count(),
                agg_sum(col_from_string('amount')),
                agg_avg(col_from_string('price')),
                agg_min(col_from_string('created_at')),
                agg_max(col_from_string('updated_at'))
            )
            ->from(table('orders'))
            ->groupBy(col_from_string('category'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT category, count(*), sum(amount), avg(price), min(created_at), max(updated_at) FROM orders GROUP BY category'
        );
    }

    public function test_select_with_aliased_columns() : void
    {
        $query = select()
            ->select(
                col_from_string('first_name')->as('fname'),
                col_from_string('last_name')->as('lname')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT first_name AS fname, last_name AS lname FROM users'
        );
    }

    public function test_select_with_all() : void
    {
        $subquery = select()
            ->select(col_from_string('price'))
            ->from(table('budget_products'));

        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(all_sub_selects(col_from_string('price'), ComparisonOperator::GT, $subquery));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > ALL (SELECT price FROM budget_products)'
        );
    }

    public function test_select_with_any() : void
    {
        $subquery = select()
            ->select(col_from_string('price'))
            ->from(table('discounted_products'));

        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(any_sub_select(col_from_string('price'), ComparisonOperator::GT, $subquery));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > ANY (SELECT price FROM discounted_products)'
        );
    }

    public function test_select_with_array_constructor() : void
    {
        $query = select()
            ->select(array_expr([literal_int(1), literal_int(2), literal_int(3)])->as('numbers'))
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT ARRAY[1, 2, 3] AS numbers FROM users'
        );
    }

    public function test_select_with_array_expression() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(is_in(col_from_string('id'), [literal_int(1), literal_int(2), literal_int(3)]));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE id IN (1, 2, 3)'
        );
    }

    public function test_select_with_between_condition() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(between(col_from_string('price'), literal_int(10), literal_int(100)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price BETWEEN 10 AND 100'
        );
    }

    public function test_select_with_case_expression() : void
    {
        $query = select()
            ->select(
                col_from_string('name'),
                case_when([
                    when(binary_expr(col_from_string('price'), '>', literal_int(100)), literal_string('expensive')),
                    when(binary_expr(col_from_string('price'), '>', literal_int(50)), literal_string('moderate')),
                ], literal_string('cheap'))->as('price_category')
            )
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT name, CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END AS price_category FROM products"
        );
    }

    public function test_select_with_coalesce() : void
    {
        $query = select()
            ->select(
                col_from_string('id'),
                coalesce(col_from_string('nickname'), col_from_string('name'), literal_string('Anonymous'))->as('display_name')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT id, COALESCE(nickname, name, 'Anonymous') AS display_name FROM users"
        );
    }

    public function test_select_with_complex_and_or_conditions() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(
                cond_and(
                    eq(col_from_string('status'), literal_string('active')),
                    cond_or(
                        gte(col_from_string('age'), literal_int(18)),
                        eq(col_from_string('guardian_approved'), literal_bool(true))
                    )
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE status = 'active' AND (age >= 18 OR guardian_approved = true)"
        );
    }

    public function test_select_with_cross_join() : void
    {
        $query = select()
            ->select(star())
            ->from(table('colors'))
            ->crossJoin(table('sizes'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM colors CROSS JOIN sizes'
        );
    }

    public function test_select_with_cte() : void
    {
        $query = select_with(with_cte([
            cte(
                'active_users',
                select()
                    ->select(col_from_string('id'), col_from_string('name'))
                    ->from(table('users'))
                    ->where(eq(col_from_string('active'), literal_bool(true)))
            ),
        ]))
            ->select(star())
            ->from(cte_ref('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_cte_materialized() : void
    {
        $cteQuery = select()
            ->select(col_from_string('id'), col_from_string('name'))
            ->from(table('users'))
            ->where(eq(col_from_string('active'), literal_bool(true)));

        $query = select_with(with_cte([
            cte('active_users', $cteQuery, [], CTEMaterialization::MATERIALIZED),
        ]))
            ->select(star())
            ->from(cte_ref('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_cte_not_materialized() : void
    {
        $cteQuery = select()
            ->select(col_from_string('id'), col_from_string('name'))
            ->from(table('users'))
            ->where(eq(col_from_string('active'), literal_bool(true)));

        $query = select_with(with_cte([
            cte('active_users', $cteQuery, [], CTEMaterialization::NOT_MATERIALIZED),
        ]))
            ->select(star())
            ->from(cte_ref('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS NOT MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_derived_table() : void
    {
        $subquery = select()
            ->select(col_from_string('user_id'), agg_sum(col_from_string('amount'))->as('total'))
            ->from(table('orders'))
            ->groupBy(col_from_string('user_id'));

        $query = select()
            ->select(col_from_string('u.name'), col_from_string('o.total'))
            ->from(table('users')->as('u'))
            ->leftJoin(
                derived($subquery, 'o'),
                eq(col_from_string('u.id'), col_from_string('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u LEFT JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id'
        );
    }

    public function test_select_with_derived_table_join() : void
    {
        $subquery = select()
            ->select(col_from_string('user_id'), agg_count()->as('order_count'))
            ->from(table('orders'))
            ->groupBy(col_from_string('user_id'));

        $query = select()
            ->select(col_from_string('users.name'), col_from_string('order_stats.order_count'))
            ->from(table('users'))
            ->join(
                derived($subquery, 'order_stats'),
                eq(col_from_string('users.id'), col_from_string('order_stats.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT users.name, order_stats.order_count FROM users JOIN (SELECT user_id, count(*) AS order_count FROM orders GROUP BY user_id) order_stats ON users.id = order_stats.user_id'
        );
    }

    public function test_select_with_distinct() : void
    {
        $query = select()
            ->selectDistinct(col_from_string('city'))
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT DISTINCT city FROM users'
        );
    }

    public function test_select_with_distinct_count() : void
    {
        $query = select()
            ->select(agg_count(col_from_string('user_id'), true)->as('unique_users'))
            ->from(table('orders'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT count(DISTINCT user_id) AS unique_users FROM orders'
        );
    }

    public function test_select_with_except() : void
    {
        $query1 = select()
            ->select(col_from_string('id'))
            ->from(table('all_users'));

        $query2 = select()
            ->select(col_from_string('id'))
            ->from(table('banned_users'));

        $query = $query1->except($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM all_users EXCEPT SELECT id FROM banned_users'
        );
    }

    public function test_select_with_exists() : void
    {
        $subquery = select()
            ->select(literal_int(1))
            ->from(table('orders'))
            ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')));

        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(exists($subquery));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)'
        );
    }

    public function test_select_with_for_key_share() : void
    {
        $query = select()
            ->select(star())
            ->from(table('accounts'))
            ->where(eq(col_from_string('id'), literal_int(1)))
            ->forKeyShare();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR KEY SHARE'
        );
    }

    public function test_select_with_for_no_key_update() : void
    {
        $query = select()
            ->select(star())
            ->from(table('accounts'))
            ->where(eq(col_from_string('id'), literal_int(1)))
            ->forNoKeyUpdate();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR NO KEY UPDATE'
        );
    }

    public function test_select_with_for_share() : void
    {
        $query = select()
            ->select(star())
            ->from(table('accounts'))
            ->where(eq(col_from_string('id'), literal_int(1)))
            ->forShare();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR SHARE'
        );
    }

    public function test_select_with_for_update() : void
    {
        $query = select()
            ->select(star())
            ->from(table('accounts'))
            ->where(eq(col_from_string('id'), literal_int(1)))
            ->forUpdate();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR UPDATE'
        );
    }

    public function test_select_with_full_join() : void
    {
        $query = select()
            ->select(star())
            ->from(table('table_a')->as('a'))
            ->fullJoin(
                table('table_b')->as('b'),
                eq(col_from_string('a.key'), col_from_string('b.key'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM table_a a FULL JOIN table_b b ON a.key = b.key'
        );
    }

    public function test_select_with_function_call() : void
    {
        $query = select()
            ->select(
                col_from_string('name'),
                func('upper', [col_from_string('name')])->as('upper_name'),
                func('length', [col_from_string('name')])->as('name_length')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT name, upper(name) AS upper_name, length(name) AS name_length FROM users'
        );
    }

    public function test_select_with_greatest() : void
    {
        $query = select()
            ->select(
                col_from_string('id'),
                greatest(col_from_string('a'), col_from_string('b'), col_from_string('c'))->as('max_value')
            )
            ->from(table('numbers'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, GREATEST(a, b, c) AS max_value FROM numbers'
        );
    }

    public function test_select_with_group_by_having() : void
    {
        $query = select()
            ->select(col_from_string('category'), agg_count()->as('cnt'))
            ->from(table('products'))
            ->groupBy(col_from_string('category'))
            ->having(gt(agg_count(), literal_int(5)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT category, count(*) AS cnt FROM products GROUP BY category HAVING count(*) > 5'
        );
    }

    public function test_select_with_in_condition() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(is_in(col_from_string('status'), [literal_string('active'), literal_string('pending'), literal_string('verified')]));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')"
        );
    }

    public function test_select_with_inner_join() : void
    {
        $query = select()
            ->select(col_from_string('u.name'), col_from_string('o.total'))
            ->from(table('users')->as('u'))
            ->join(
                table('orders')->as('o'),
                eq(col_from_string('u.id'), col_from_string('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id'
        );
    }

    public function test_select_with_intersect() : void
    {
        $query1 = select()
            ->select(col_from_string('id'))
            ->from(table('premium_users'));

        $query2 = select()
            ->select(col_from_string('id'))
            ->from(table('active_users'));

        $query = $query1->intersect($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM premium_users INTERSECT SELECT id FROM active_users'
        );
    }

    public function test_select_with_is_distinct_from() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(is_distinct_from(col_from_string('status'), literal_null()));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE status IS DISTINCT FROM NULL'
        );
    }

    public function test_select_with_is_not_distinct_from() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(is_distinct_from(col_from_string('status'), literal_string('active'), true));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE status IS NOT DISTINCT FROM 'active'"
        );
    }

    public function test_select_with_is_null_condition() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(null === col_from_string('deleted_at'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE deleted_at IS NULL'
        );
    }

    public function test_select_with_lateral_join() : void
    {
        $lateralQuery = select()
            ->select(star())
            ->from(table('orders'))
            ->where(eq(col_from_string('orders.user_id'), col_from_string('u.id')))
            ->orderBy(desc(col_from_string('created_at')))
            ->limit(3);

        $query = select()
            ->select(col_from_string('u.name'), col_from_string('recent.id'))
            ->from(table('users')->as('u'))
            ->leftJoin(
                lateral(derived($lateralQuery, 'recent')),
                raw_cond('true')
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, recent.id FROM users u LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = u.id ORDER BY created_at DESC LIMIT 3) recent ON true'
        );
    }

    public function test_select_with_lateral_subquery() : void
    {
        $subquery = select()
            ->select(col_from_string('order_id'))
            ->from(table('orders'))
            ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')))
            ->orderBy(desc(col_from_string('created_at')))
            ->limit(5);

        $query = select()
            ->select(col_from_string('users.name'), col_from_string('recent_orders.order_id'))
            ->from(table('users'))
            ->crossJoin(lateral(derived($subquery, 'recent_orders')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT users.name, recent_orders.order_id FROM users CROSS JOIN LATERAL (SELECT order_id FROM orders WHERE orders.user_id = users.id ORDER BY created_at DESC LIMIT 5) recent_orders'
        );
    }

    public function test_select_with_least() : void
    {
        $query = select()
            ->select(
                col_from_string('id'),
                least(col_from_string('a'), col_from_string('b'), col_from_string('c'))->as('min_value')
            )
            ->from(table('numbers'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, LEAST(a, b, c) AS min_value FROM numbers'
        );
    }

    public function test_select_with_left_join() : void
    {
        $query = select()
            ->select(col_from_string('u.name'), col_from_string('o.total'))
            ->from(table('users')->as('u'))
            ->leftJoin(
                table('orders')->as('o'),
                eq(col_from_string('u.id'), col_from_string('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id'
        );
    }

    public function test_select_with_like_condition() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(like(col_from_string('email'), literal_string('%@example.com')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE email LIKE '%@example.com'"
        );
    }

    public function test_select_with_limit_offset() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->orderBy(asc(col_from_string('id')))
            ->limit(10)
            ->offset(20);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20'
        );
    }

    public function test_select_with_multiple_conditions() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(
                cond_and(
                    eq(col_from_string('active'), literal_bool(true)),
                    gte(col_from_string('age'), literal_int(18)),
                    neq(col_from_string('status'), literal_string('banned'))
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE active = true AND age >= 18 AND status <> 'banned'"
        );
    }

    public function test_select_with_not_condition() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(cond_not(eq(col_from_string('status'), literal_string('banned'))));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE NOT status = 'banned'"
        );
    }

    public function test_select_with_nullif() : void
    {
        $query = select()
            ->select(
                col_from_string('id'),
                nullif(col_from_string('value'), literal_int(0))->as('safe_value')
            )
            ->from(table('data'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, NULLIF(value, 0) AS safe_value FROM data'
        );
    }

    public function test_select_with_order_by_asc_desc() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->orderBy(
                asc(col_from_string('last_name')),
                desc(col_from_string('first_name'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users ORDER BY last_name ASC, first_name DESC'
        );
    }

    public function test_select_with_order_by_nulls_first_last() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->orderBy(
                order_by(col_from_string('price'), SortDirection::ASC, NullsPosition::FIRST),
                order_by(col_from_string('name'), SortDirection::DESC, NullsPosition::LAST)
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products ORDER BY price ASC NULLS FIRST, name DESC NULLS LAST'
        );
    }

    public function test_select_with_parameter() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(eq(col_from_string('id'), param(1)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE id = $1'
        );
    }

    public function test_select_with_raw_expression() : void
    {
        $query = select()
            ->select(
                star(),
                binary_expr(literal_int(1), '+', literal_int(1))->as('calculated')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT *, 1 + 1 AS calculated FROM users'
        );
    }

    public function test_select_with_raw_expression_in_select() : void
    {
        $query = select()
            ->select(
                col_from_string('id'),
                raw_expr('1 + 1')->as('two')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, 1 + 1 AS two FROM users'
        );
    }

    public function test_select_with_right_join() : void
    {
        $query = select()
            ->select(star())
            ->from(table('orders')->as('o'))
            ->rightJoin(
                table('users')->as('u'),
                eq(col_from_string('o.user_id'), col_from_string('u.id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM orders o RIGHT JOIN users u ON o.user_id = u.id'
        );
    }

    public function test_select_with_row_expression() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(
                eq(
                    row_expr([col_from_string('first_name'), col_from_string('last_name')]),
                    row_expr([literal_string('John'), literal_string('Doe')])
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE (first_name, last_name) = ('John', 'Doe')"
        );
    }

    public function test_select_with_scalar_subquery() : void
    {
        $subquery = select()
            ->select(agg_count())
            ->from(table('orders'))
            ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')));

        $query = select()
            ->select(
                col_from_string('name'),
                sub_select($subquery)->as('order_count')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users'
        );
    }

    public function test_select_with_schema_qualified_table() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users', 'public'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM public.users'
        );
    }

    public function test_select_with_similar_to() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(similar_to(col_from_string('name'), literal_string('%John%')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE name SIMILAR TO '%John%'"
        );
    }

    public function test_select_with_table_function() : void
    {
        $query = select()
            ->select(col_from_string('value'))
            ->from(table_func(func('generate_series', [literal_int(1), literal_int(10)]))->as('value'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT value FROM generate_series(1, 10) value'
        );
    }

    public function test_select_with_type_cast() : void
    {
        $query = select()
            ->select(
                col_from_string('id'),
                cast(col_from_string('price'), 'integer')->as('price_int')
            )
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, price::"integer" AS price_int FROM products'
        );
    }

    public function test_select_with_union() : void
    {
        $query1 = select()
            ->select(col_from_string('name'))
            ->from(table('users'));

        $query2 = select()
            ->select(col_from_string('name'))
            ->from(table('admins'));

        $query = $query1->union($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT name FROM users UNION SELECT name FROM admins'
        );
    }

    public function test_select_with_union_all() : void
    {
        $query1 = select()
            ->select(col_from_string('id'))
            ->from(table('table1'));

        $query2 = select()
            ->select(col_from_string('id'))
            ->from(table('table2'));

        $query = $query1->unionAll($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM table1 UNION ALL SELECT id FROM table2'
        );
    }

    public function test_select_with_where_comparison_operators() : void
    {
        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(
                cond_and(
                    gt(col_from_string('price'), literal_int(10)),
                    lt(col_from_string('price'), literal_int(100)),
                    gte(col_from_string('quantity'), literal_int(1)),
                    lte(col_from_string('weight'), literal_int(50))
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > 10 AND price < 100 AND quantity >= 1 AND weight <= 50'
        );
    }

    public function test_select_with_window_definition() : void
    {
        $query = select()
            ->select(
                col_from_string('department'),
                col_from_string('salary'),
                window_func('row_number', [], [], [order_by(col_from_string('salary'), SortDirection::DESC)])->as('rank')
            )
            ->from(table('employees'))
            ->window(window_def('w', [col_from_string('department')], [order_by(col_from_string('salary'), SortDirection::DESC)]));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT department, salary, row_number() OVER (ORDER BY salary DESC) AS rank FROM employees WINDOW w AS (PARTITION BY department ORDER BY salary DESC)'
        );
    }

    public function test_simple_select() : void
    {
        $query = select()
            ->select(col_from_string('id'), col_from_string('name'))
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, name FROM users'
        );
    }

    public function test_simple_select_star() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users'
        );
    }

    public function test_simple_select_with_where() : void
    {
        $query = select()
            ->select(star())
            ->from(table('users'))
            ->where(eq(col_from_string('active'), literal_bool(true)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE active = true'
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    pg_all,
    pg_and,
    pg_any,
    pg_array,
    pg_asc,
    pg_avg,
    pg_between,
    pg_binary,
    pg_bool,
    pg_case,
    pg_cast,
    pg_coalesce,
    pg_col,
    pg_count,
    pg_cte,
    pg_cte_ref,
    pg_derived,
    pg_desc,
    pg_eq,
    pg_exists,
    pg_func,
    pg_greatest,
    pg_gt,
    pg_gte,
    pg_in,
    pg_int,
    pg_is_distinct_from,
    pg_is_null,
    pg_lateral,
    pg_least,
    pg_like,
    pg_lt,
    pg_lte,
    pg_max,
    pg_min,
    pg_neq,
    pg_not,
    pg_null,
    pg_nullif,
    pg_or,
    pg_order,
    pg_param,
    pg_raw,
    pg_raw_condition,
    pg_row,
    pg_select,
    pg_select_with,
    pg_similar_to,
    pg_star,
    pg_string,
    pg_subquery,
    pg_sum,
    pg_table,
    pg_table_func,
    pg_when,
    pg_window_def,
    pg_window_func,
    pg_with
};
use Flow\PgQuery\Protobuf\AST\Node;

use Flow\PgQuery\QueryBuilder\Clause\{CTEMaterialization, NullsPosition, SortDirection};

use Flow\PgQuery\QueryBuilder\Condition\ComparisonOperator;

final class SelectBuilderTest extends PGQueryTestCase
{
    public function test_select_distinct_on() : void
    {
        $query = pg_select()
            ->selectDistinctOn(
                [pg_col('department')],
                pg_col('name'),
                pg_col('salary')
            )
            ->from(pg_table('employees'))
            ->orderBy(pg_asc(pg_col('department')), pg_desc(pg_col('salary')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT DISTINCT ON (department) name, salary FROM employees ORDER BY department ASC, salary DESC'
        );
    }

    public function test_select_except_all() : void
    {
        $query1 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('all_users'));

        $query2 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('banned_users'));

        $query = $query1->exceptAll($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM all_users EXCEPT ALL SELECT id FROM banned_users'
        );
    }

    public function test_select_intersect_all() : void
    {
        $query1 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('users'));

        $query2 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('admins'));

        $query = $query1->intersectAll($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM users INTERSECT ALL SELECT id FROM admins'
        );
    }

    public function test_select_with_aggregates() : void
    {
        $query = pg_select()
            ->select(
                pg_col('category'),
                pg_count(),
                pg_sum(pg_col('amount')),
                pg_avg(pg_col('price')),
                pg_min(pg_col('created_at')),
                pg_max(pg_col('updated_at'))
            )
            ->from(pg_table('orders'))
            ->groupBy(pg_col('category'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT category, count(*), sum(amount), avg(price), min(created_at), max(updated_at) FROM orders GROUP BY category'
        );
    }

    public function test_select_with_aliased_columns() : void
    {
        $query = pg_select()
            ->select(
                pg_col('first_name')->as('fname'),
                pg_col('last_name')->as('lname')
            )
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT first_name AS fname, last_name AS lname FROM users'
        );
    }

    public function test_select_with_all() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_col('price'))
            ->from(pg_table('budget_products'));
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('products'))
            ->where(pg_all(pg_col('price'), ComparisonOperator::GT, $subqueryNode));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > ALL (SELECT price FROM budget_products)'
        );
    }

    public function test_select_with_any() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_col('price'))
            ->from(pg_table('discounted_products'));
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('products'))
            ->where(pg_any(pg_col('price'), ComparisonOperator::GT, $subqueryNode));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > ANY (SELECT price FROM discounted_products)'
        );
    }

    public function test_select_with_array_constructor() : void
    {
        $query = pg_select()
            ->select(pg_array([pg_int(1), pg_int(2), pg_int(3)])->as('numbers'))
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT ARRAY[1, 2, 3] AS numbers FROM users'
        );
    }

    public function test_select_with_array_expression() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_in(pg_col('id'), [pg_int(1), pg_int(2), pg_int(3)]));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE id IN (1, 2, 3)'
        );
    }

    public function test_select_with_between_condition() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('products'))
            ->where(pg_between(pg_col('price'), pg_int(10), pg_int(100)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price BETWEEN 10 AND 100'
        );
    }

    public function test_select_with_case_expression() : void
    {
        $query = pg_select()
            ->select(
                pg_col('name'),
                pg_case([
                    pg_when(pg_binary(pg_col('price'), '>', pg_int(100)), pg_string('expensive')),
                    pg_when(pg_binary(pg_col('price'), '>', pg_int(50)), pg_string('moderate')),
                ], pg_string('cheap'))->as('price_category')
            )
            ->from(pg_table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT name, CASE WHEN price > 100 THEN 'expensive' WHEN price > 50 THEN 'moderate' ELSE 'cheap' END AS price_category FROM products"
        );
    }

    public function test_select_with_coalesce() : void
    {
        $query = pg_select()
            ->select(
                pg_col('id'),
                pg_coalesce(pg_col('nickname'), pg_col('name'), pg_string('Anonymous'))->as('display_name')
            )
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT id, COALESCE(nickname, name, 'Anonymous') AS display_name FROM users"
        );
    }

    public function test_select_with_complex_and_or_conditions() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(
                pg_and(
                    pg_eq(pg_col('status'), pg_string('active')),
                    pg_or(
                        pg_gte(pg_col('age'), pg_int(18)),
                        pg_eq(pg_col('guardian_approved'), pg_bool(true))
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
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('colors'))
            ->crossJoin(pg_table('sizes'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM colors CROSS JOIN sizes'
        );
    }

    public function test_select_with_cte() : void
    {
        $cteNode = new Node();
        $cteQuery = pg_select()
            ->select(pg_col('id'), pg_col('name'))
            ->from(pg_table('users'))
            ->where(pg_eq(pg_col('active'), pg_bool(true)));
        $cteNode->setSelectStmt($cteQuery->toAst());

        $query = pg_select_with(pg_with([
            pg_cte('active_users', $cteNode),
        ]))
            ->select(pg_star())
            ->from(pg_cte_ref('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_cte_materialized() : void
    {
        $cteNode = new Node();
        $cteQuery = pg_select()
            ->select(pg_col('id'), pg_col('name'))
            ->from(pg_table('users'))
            ->where(pg_eq(pg_col('active'), pg_bool(true)));
        $cteNode->setSelectStmt($cteQuery->toAst());

        $query = pg_select_with(pg_with([
            pg_cte('active_users', $cteNode, [], CTEMaterialization::MATERIALIZED),
        ]))
            ->select(pg_star())
            ->from(pg_cte_ref('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_cte_not_materialized() : void
    {
        $cteNode = new Node();
        $cteQuery = pg_select()
            ->select(pg_col('id'), pg_col('name'))
            ->from(pg_table('users'))
            ->where(pg_eq(pg_col('active'), pg_bool(true)));
        $cteNode->setSelectStmt($cteQuery->toAst());

        $query = pg_select_with(pg_with([
            pg_cte('active_users', $cteNode, [], CTEMaterialization::NOT_MATERIALIZED),
        ]))
            ->select(pg_star())
            ->from(pg_cte_ref('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS NOT MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_derived_table() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_col('user_id'), pg_sum(pg_col('amount'))->as('total'))
            ->from(pg_table('orders'))
            ->groupBy(pg_col('user_id'));
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(pg_col('u.name'), pg_col('o.total'))
            ->from(pg_table('users')->as('u'))
            ->leftJoin(
                pg_derived($subqueryNode, 'o'),
                pg_eq(pg_col('u.id'), pg_col('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u LEFT JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id'
        );
    }

    public function test_select_with_derived_table_join() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_col('user_id'), pg_count()->as('order_count'))
            ->from(pg_table('orders'))
            ->groupBy(pg_col('user_id'));
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(pg_col('users.name'), pg_col('order_stats.order_count'))
            ->from(pg_table('users'))
            ->join(
                pg_derived($subqueryNode, 'order_stats'),
                pg_eq(pg_col('users.id'), pg_col('order_stats.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT users.name, order_stats.order_count FROM users JOIN (SELECT user_id, count(*) AS order_count FROM orders GROUP BY user_id) order_stats ON users.id = order_stats.user_id'
        );
    }

    public function test_select_with_distinct() : void
    {
        $query = pg_select()
            ->selectDistinct(pg_col('city'))
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT DISTINCT city FROM users'
        );
    }

    public function test_select_with_distinct_count() : void
    {
        $query = pg_select()
            ->select(pg_count(pg_col('user_id'), true)->as('unique_users'))
            ->from(pg_table('orders'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT count(DISTINCT user_id) AS unique_users FROM orders'
        );
    }

    public function test_select_with_except() : void
    {
        $query1 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('all_users'));

        $query2 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('banned_users'));

        $query = $query1->except($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM all_users EXCEPT SELECT id FROM banned_users'
        );
    }

    public function test_select_with_exists() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_int(1))
            ->from(pg_table('orders'))
            ->where(pg_eq(pg_col('orders.user_id'), pg_col('users.id')));
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_exists($subqueryNode));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)'
        );
    }

    public function test_select_with_for_key_share() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('accounts'))
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->forKeyShare();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR KEY SHARE'
        );
    }

    public function test_select_with_for_no_key_update() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('accounts'))
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->forNoKeyUpdate();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR NO KEY UPDATE'
        );
    }

    public function test_select_with_for_share() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('accounts'))
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->forShare();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR SHARE'
        );
    }

    public function test_select_with_for_update() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('accounts'))
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->forUpdate();

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM accounts WHERE id = 1 FOR UPDATE'
        );
    }

    public function test_select_with_full_join() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('table_a')->as('a'))
            ->fullJoin(
                pg_table('table_b')->as('b'),
                pg_eq(pg_col('a.key'), pg_col('b.key'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM table_a a FULL JOIN table_b b ON a.key = b.key'
        );
    }

    public function test_select_with_function_call() : void
    {
        $query = pg_select()
            ->select(
                pg_col('name'),
                pg_func('upper', [pg_col('name')])->as('upper_name'),
                pg_func('length', [pg_col('name')])->as('name_length')
            )
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT name, upper(name) AS upper_name, length(name) AS name_length FROM users'
        );
    }

    public function test_select_with_greatest() : void
    {
        $query = pg_select()
            ->select(
                pg_col('id'),
                pg_greatest(pg_col('a'), pg_col('b'), pg_col('c'))->as('max_value')
            )
            ->from(pg_table('numbers'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, GREATEST(a, b, c) AS max_value FROM numbers'
        );
    }

    public function test_select_with_group_by_having() : void
    {
        $query = pg_select()
            ->select(pg_col('category'), pg_count()->as('cnt'))
            ->from(pg_table('products'))
            ->groupBy(pg_col('category'))
            ->having(pg_gt(pg_count(), pg_int(5)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT category, count(*) AS cnt FROM products GROUP BY category HAVING count(*) > 5'
        );
    }

    public function test_select_with_in_condition() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_in(pg_col('status'), [pg_string('active'), pg_string('pending'), pg_string('verified')]));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')"
        );
    }

    public function test_select_with_inner_join() : void
    {
        $query = pg_select()
            ->select(pg_col('u.name'), pg_col('o.total'))
            ->from(pg_table('users')->as('u'))
            ->join(
                pg_table('orders')->as('o'),
                pg_eq(pg_col('u.id'), pg_col('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id'
        );
    }

    public function test_select_with_intersect() : void
    {
        $query1 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('premium_users'));

        $query2 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('active_users'));

        $query = $query1->intersect($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM premium_users INTERSECT SELECT id FROM active_users'
        );
    }

    public function test_select_with_is_distinct_from() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_is_distinct_from(pg_col('status'), pg_null()));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE status IS DISTINCT FROM NULL'
        );
    }

    public function test_select_with_is_not_distinct_from() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_is_distinct_from(pg_col('status'), pg_string('active'), true));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE status IS NOT DISTINCT FROM 'active'"
        );
    }

    public function test_select_with_is_null_condition() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_is_null(pg_col('deleted_at')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE deleted_at IS NULL'
        );
    }

    public function test_select_with_lateral_join() : void
    {
        $lateralSubqueryNode = new Node();
        $lateralQuery = pg_select()
            ->select(pg_star())
            ->from(pg_table('orders'))
            ->where(pg_eq(pg_col('orders.user_id'), pg_col('u.id')))
            ->orderBy(pg_desc(pg_col('created_at')))
            ->limit(3);
        $lateralSubqueryNode->setSelectStmt($lateralQuery->toAst());

        $query = pg_select()
            ->select(pg_col('u.name'), pg_col('recent.id'))
            ->from(pg_table('users')->as('u'))
            ->leftJoin(
                pg_lateral(pg_derived($lateralSubqueryNode, 'recent')),
                pg_raw_condition('true')
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, recent.id FROM users u LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = u.id ORDER BY created_at DESC LIMIT 3) recent ON true'
        );
    }

    public function test_select_with_lateral_subquery() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_col('order_id'))
            ->from(pg_table('orders'))
            ->where(pg_eq(pg_col('orders.user_id'), pg_col('users.id')))
            ->orderBy(pg_desc(pg_col('created_at')))
            ->limit(5);
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(pg_col('users.name'), pg_col('recent_orders.order_id'))
            ->from(pg_table('users'))
            ->crossJoin(pg_lateral(pg_derived($subqueryNode, 'recent_orders')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT users.name, recent_orders.order_id FROM users CROSS JOIN LATERAL (SELECT order_id FROM orders WHERE orders.user_id = users.id ORDER BY created_at DESC LIMIT 5) recent_orders'
        );
    }

    public function test_select_with_least() : void
    {
        $query = pg_select()
            ->select(
                pg_col('id'),
                pg_least(pg_col('a'), pg_col('b'), pg_col('c'))->as('min_value')
            )
            ->from(pg_table('numbers'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, LEAST(a, b, c) AS min_value FROM numbers'
        );
    }

    public function test_select_with_left_join() : void
    {
        $query = pg_select()
            ->select(pg_col('u.name'), pg_col('o.total'))
            ->from(pg_table('users')->as('u'))
            ->leftJoin(
                pg_table('orders')->as('o'),
                pg_eq(pg_col('u.id'), pg_col('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id'
        );
    }

    public function test_select_with_like_condition() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_like(pg_col('email'), pg_string('%@example.com')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE email LIKE '%@example.com'"
        );
    }

    public function test_select_with_limit_offset() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->orderBy(pg_asc(pg_col('id')))
            ->limit(10)
            ->offset(20);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20'
        );
    }

    public function test_select_with_multiple_conditions() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(
                pg_and(
                    pg_eq(pg_col('active'), pg_bool(true)),
                    pg_gte(pg_col('age'), pg_int(18)),
                    pg_neq(pg_col('status'), pg_string('banned'))
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE active = true AND age >= 18 AND status <> 'banned'"
        );
    }

    public function test_select_with_not_condition() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_not(pg_eq(pg_col('status'), pg_string('banned'))));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE NOT status = 'banned'"
        );
    }

    public function test_select_with_nullif() : void
    {
        $query = pg_select()
            ->select(
                pg_col('id'),
                pg_nullif(pg_col('value'), pg_int(0))->as('safe_value')
            )
            ->from(pg_table('data'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, NULLIF(value, 0) AS safe_value FROM data'
        );
    }

    public function test_select_with_order_by_asc_desc() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->orderBy(
                pg_asc(pg_col('last_name')),
                pg_desc(pg_col('first_name'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users ORDER BY last_name ASC, first_name DESC'
        );
    }

    public function test_select_with_order_by_nulls_first_last() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('products'))
            ->orderBy(
                pg_order(pg_col('price'), SortDirection::ASC, NullsPosition::FIRST),
                pg_order(pg_col('name'), SortDirection::DESC, NullsPosition::LAST)
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products ORDER BY price ASC NULLS FIRST, name DESC NULLS LAST'
        );
    }

    public function test_select_with_parameter() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_eq(pg_col('id'), pg_param(1)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE id = $1'
        );
    }

    public function test_select_with_raw_expression() : void
    {
        $query = pg_select()
            ->select(
                pg_star(),
                pg_binary(pg_int(1), '+', pg_int(1))->as('calculated')
            )
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT *, 1 + 1 AS calculated FROM users'
        );
    }

    public function test_select_with_raw_expression_in_select() : void
    {
        $query = pg_select()
            ->select(
                pg_col('id'),
                pg_raw('1 + 1')->as('two')
            )
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, 1 + 1 AS two FROM users'
        );
    }

    public function test_select_with_right_join() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('orders')->as('o'))
            ->rightJoin(
                pg_table('users')->as('u'),
                pg_eq(pg_col('o.user_id'), pg_col('u.id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM orders o RIGHT JOIN users u ON o.user_id = u.id'
        );
    }

    public function test_select_with_row_expression() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(
                pg_eq(
                    pg_row([pg_col('first_name'), pg_col('last_name')]),
                    pg_row([pg_string('John'), pg_string('Doe')])
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE (first_name, last_name) = ('John', 'Doe')"
        );
    }

    public function test_select_with_scalar_subquery() : void
    {
        $subqueryNode = new Node();
        $subquery = pg_select()
            ->select(pg_count())
            ->from(pg_table('orders'))
            ->where(pg_eq(pg_col('orders.user_id'), pg_col('users.id')));
        $subqueryNode->setSelectStmt($subquery->toAst());

        $query = pg_select()
            ->select(
                pg_col('name'),
                pg_subquery($subqueryNode)->as('order_count')
            )
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users'
        );
    }

    public function test_select_with_schema_qualified_table() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users', 'public'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM public.users'
        );
    }

    public function test_select_with_similar_to() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_similar_to(pg_col('name'), pg_string('%John%')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE name SIMILAR TO '%John%'"
        );
    }

    public function test_select_with_table_function() : void
    {
        $query = pg_select()
            ->select(pg_col('value'))
            ->from(pg_table_func(pg_func('generate_series', [pg_int(1), pg_int(10)]))->as('value'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT value FROM generate_series(1, 10) value'
        );
    }

    public function test_select_with_type_cast() : void
    {
        $query = pg_select()
            ->select(
                pg_col('id'),
                pg_cast(pg_col('price'), 'integer')->as('price_int')
            )
            ->from(pg_table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, price::"integer" AS price_int FROM products'
        );
    }

    public function test_select_with_union() : void
    {
        $query1 = pg_select()
            ->select(pg_col('name'))
            ->from(pg_table('users'));

        $query2 = pg_select()
            ->select(pg_col('name'))
            ->from(pg_table('admins'));

        $query = $query1->union($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT name FROM users UNION SELECT name FROM admins'
        );
    }

    public function test_select_with_union_all() : void
    {
        $query1 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('table1'));

        $query2 = pg_select()
            ->select(pg_col('id'))
            ->from(pg_table('table2'));

        $query = $query1->unionAll($query2);

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id FROM table1 UNION ALL SELECT id FROM table2'
        );
    }

    public function test_select_with_where_comparison_operators() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('products'))
            ->where(
                pg_and(
                    pg_gt(pg_col('price'), pg_int(10)),
                    pg_lt(pg_col('price'), pg_int(100)),
                    pg_gte(pg_col('quantity'), pg_int(1)),
                    pg_lte(pg_col('weight'), pg_int(50))
                )
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > 10 AND price < 100 AND quantity >= 1 AND weight <= 50'
        );
    }

    public function test_select_with_window_definition() : void
    {
        $query = pg_select()
            ->select(
                pg_col('department'),
                pg_col('salary'),
                pg_window_func('row_number', [], [], [pg_order(pg_col('salary'), SortDirection::DESC)])->as('rank')
            )
            ->from(pg_table('employees'))
            ->window(pg_window_def('w', [pg_col('department')], [pg_order(pg_col('salary'), SortDirection::DESC)]));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT department, salary, row_number() OVER (ORDER BY salary DESC) AS rank FROM employees WINDOW w AS (PARTITION BY department ORDER BY salary DESC)'
        );
    }

    public function test_simple_select() : void
    {
        $query = pg_select()
            ->select(pg_col('id'), pg_col('name'))
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, name FROM users'
        );
    }

    public function test_simple_select_star() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users'
        );
    }

    public function test_simple_select_with_where() : void
    {
        $query = pg_select()
            ->select(pg_star())
            ->from(pg_table('users'))
            ->where(pg_eq(pg_col('active'), pg_bool(true)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE active = true'
        );
    }
}

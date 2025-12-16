<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{
    agg_avg,
    agg_count,
    agg_max,
    agg_min,
    agg_sum,
    all_sub_select,
    any_sub_select,
    array_expr,
    asc,
    between,
    binary_expr,
    case_when,
    cast,
    coalesce,
    col,
    cond_and,
    cond_not,
    cond_or,
    cte,
    current_date,
    current_time,
    current_timestamp,
    data_type_integer,
    data_type_text,
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
    is_null,
    lateral,
    least,
    like,
    literal,
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
use Flow\PostgreSql\QueryBuilder\Clause\{CTEMaterialization, NullsPosition, SortDirection};

use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;

final class SelectBuilderTest extends PGQueryTestCase
{
    public function test_select_distinct_on() : void
    {
        $query = select()
            ->selectDistinctOn(
                [col('department')],
                col('name'),
                col('salary')
            )
            ->from(table('employees'))
            ->orderBy(asc(col('department')), desc(col('salary')));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT DISTINCT ON (department) name, salary FROM employees ORDER BY department ASC, salary DESC'
        );
    }

    public function test_select_except_all() : void
    {
        $query1 = select()
            ->select(col('id'))
            ->from(table('all_users'));

        $query2 = select()
            ->select(col('id'))
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
            ->select(col('id'))
            ->from(table('users'));

        $query2 = select()
            ->select(col('id'))
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
                col('category'),
                agg_count(),
                agg_sum(col('amount')),
                agg_avg(col('price')),
                agg_min(col('created_at')),
                agg_max(col('updated_at'))
            )
            ->from(table('orders'))
            ->groupBy(col('category'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT category, count(*), sum(amount), avg(price), min(created_at), max(updated_at) FROM orders GROUP BY category'
        );
    }

    public function test_select_with_aliased_columns() : void
    {
        $query = select()
            ->select(
                col('first_name')->as('fname'),
                col('last_name')->as('lname')
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
            ->select(col('price'))
            ->from(table('budget_products'));

        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(all_sub_select(col('price'), ComparisonOperator::GT, $subquery));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > ALL (SELECT price FROM budget_products)'
        );
    }

    public function test_select_with_any() : void
    {
        $subquery = select()
            ->select(col('price'))
            ->from(table('discounted_products'));

        $query = select()
            ->select(star())
            ->from(table('products'))
            ->where(any_sub_select(col('price'), ComparisonOperator::GT, $subquery));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price > ANY (SELECT price FROM discounted_products)'
        );
    }

    public function test_select_with_array_constructor() : void
    {
        $query = select()
            ->select(array_expr([literal(1), literal(2), literal(3)])->as('numbers'))
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
            ->where(is_in(col('id'), [literal(1), literal(2), literal(3)]));

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
            ->where(between(col('price'), literal(10), literal(100)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM products WHERE price BETWEEN 10 AND 100'
        );
    }

    public function test_select_with_case_expression() : void
    {
        $query = select()
            ->select(
                col('name'),
                case_when([
                    when(binary_expr(col('price'), '>', literal(100)), literal('expensive')),
                    when(binary_expr(col('price'), '>', literal(50)), literal('moderate')),
                ], literal('cheap'))->as('price_category')
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
                col('id'),
                coalesce(
                    col('nickname'),
                    col('name'),
                    literal('Anonymous')
                )->as('display_name')
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
                    eq(col('status'), literal('active')),
                    cond_or(
                        gte(col('age'), literal(18)),
                        eq(col('guardian_approved'), literal(true))
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
        $query = with(
            cte(
                'active_users',
                select()
                    ->select(col('id'), col('name'))
                    ->from(table('users'))
                    ->where(eq(col('active'), literal(true)))
            )
        )
            ->select(star())
            ->from(table('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_cte_materialized() : void
    {
        $cteQuery = select()
            ->select(col('id'), col('name'))
            ->from(table('users'))
            ->where(eq(col('active'), literal(true)));

        $query = with(cte('active_users', $cteQuery, [], CTEMaterialization::MATERIALIZED))
            ->select(star())
            ->from(table('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_cte_not_materialized() : void
    {
        $cteQuery = select()
            ->select(col('id'), col('name'))
            ->from(table('users'))
            ->where(eq(col('active'), literal(true)));

        $query = with(cte('active_users', $cteQuery, [], CTEMaterialization::NOT_MATERIALIZED))
            ->select(star())
            ->from(table('active_users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'WITH active_users AS NOT MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users'
        );
    }

    public function test_select_with_derived_table() : void
    {
        $subquery = select()
            ->select(col('user_id'), agg_sum(col('amount'))->as('total'))
            ->from(table('orders'))
            ->groupBy(col('user_id'));

        $query = select()
            ->select(col('u.name'), col('o.total'))
            ->from(table('users')->as('u'))
            ->leftJoin(
                derived($subquery, 'o'),
                eq(col('u.id'), col('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u LEFT JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id'
        );
    }

    public function test_select_with_derived_table_join() : void
    {
        $subquery = select()
            ->select(col('user_id'), agg_count()->as('order_count'))
            ->from(table('orders'))
            ->groupBy(col('user_id'));

        $query = select()
            ->select(col('users.name'), col('order_stats.order_count'))
            ->from(table('users'))
            ->join(
                derived($subquery, 'order_stats'),
                eq(col('users.id'), col('order_stats.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT users.name, order_stats.order_count FROM users JOIN (SELECT user_id, count(*) AS order_count FROM orders GROUP BY user_id) order_stats ON users.id = order_stats.user_id'
        );
    }

    public function test_select_with_distinct() : void
    {
        $query = select()
            ->selectDistinct(col('city'))
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT DISTINCT city FROM users'
        );
    }

    public function test_select_with_distinct_count() : void
    {
        $query = select()
            ->select(agg_count(col('user_id'), true)->as('unique_users'))
            ->from(table('orders'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT count(DISTINCT user_id) AS unique_users FROM orders'
        );
    }

    public function test_select_with_except() : void
    {
        $query1 = select()
            ->select(col('id'))
            ->from(table('all_users'));

        $query2 = select()
            ->select(col('id'))
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
            ->select(literal(1))
            ->from(table('orders'))
            ->where(eq(col('orders.user_id'), col('users.id')));

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
            ->where(eq(col('id'), literal(1)))
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
            ->where(eq(col('id'), literal(1)))
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
            ->where(eq(col('id'), literal(1)))
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
            ->where(eq(col('id'), literal(1)))
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
                eq(col('a.key'), col('b.key'))
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
                col('name'),
                func('upper', [col('name')])->as('upper_name'),
                func('length', [col('name')])->as('name_length')
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
                col('id'),
                greatest(col('a'), col('b'), col('c'))->as('max_value')
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
            ->select(col('category'), agg_count()->as('cnt'))
            ->from(table('products'))
            ->groupBy(col('category'))
            ->having(gt(agg_count(), literal(5)));

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
            ->where(is_in(
                col('status'),
                [literal('active'), literal('pending'), literal('verified')]
            ));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')"
        );
    }

    public function test_select_with_inner_join() : void
    {
        $query = select()
            ->select(col('u.name'), col('o.total'))
            ->from(table('users')->as('u'))
            ->join(
                table('orders')->as('o'),
                eq(col('u.id'), col('o.user_id'))
            );

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id'
        );
    }

    public function test_select_with_intersect() : void
    {
        $query1 = select()
            ->select(col('id'))
            ->from(table('premium_users'));

        $query2 = select()
            ->select(col('id'))
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
            ->where(is_distinct_from(col('status'), literal(null)));

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
            ->where(is_distinct_from(col('status'), literal('active'), true));

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
            ->where(is_null(col('deleted_at')));

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
            ->where(eq(col('orders.user_id'), col('u.id')))
            ->orderBy(desc(col('created_at')))
            ->limit(3);

        $query = select()
            ->select(col('u.name'), col('recent.id'))
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
            ->select(col('order_id'))
            ->from(table('orders'))
            ->where(eq(col('orders.user_id'), col('users.id')))
            ->orderBy(desc(col('created_at')))
            ->limit(5);

        $query = select()
            ->select(col('users.name'), col('recent_orders.order_id'))
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
                col('id'),
                least(col('a'), col('b'), col('c'))->as('min_value')
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
            ->select(col('u.name'), col('o.total'))
            ->from(table('users')->as('u'))
            ->leftJoin(
                table('orders')->as('o'),
                eq(col('u.id'), col('o.user_id'))
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
            ->where(like(col('email'), literal('%@example.com')));

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
            ->orderBy(asc(col('id')))
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
                    eq(col('active'), literal(true)),
                    gte(col('age'), literal(18)),
                    neq(col('status'), literal('banned'))
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
            ->where(cond_not(eq(col('status'), literal('banned'))));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE NOT status = 'banned'"
        );
    }

    public function test_select_with_nullif() : void
    {
        $query = select()
            ->select(
                col('id'),
                nullif(col('value'), literal(0))->as('safe_value')
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
                asc(col('last_name')),
                desc(col('first_name'))
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
                order_by(col('price'), SortDirection::ASC, NullsPosition::FIRST),
                order_by(col('name'), SortDirection::DESC, NullsPosition::LAST)
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
            ->where(eq(col('id'), param(1)));

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
                binary_expr(literal(1), '+', literal(1))->as('calculated')
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
                col('id'),
                raw_expr('1 + 1')->as('two')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, 1 + 1 AS two FROM users'
        );
    }

    public function test_select_with_recursive_cte_window_functions_and_case_expression() : void
    {
        $orgTreeAnchor = select()
            ->select(
                col('id'),
                col('name'),
                col('manager_id'),
                literal(0)->as('level'),
                cast(col('name'), data_type_text())->as('path')
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
                window_func(
                    'row_number',
                    [],
                    [col('e.department_id')],
                    [desc(col('e.salary'))]
                ),
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
            ->join(
                table('dept_stats')->as('ds'),
                eq(col('e.department_id'), col('ds.department_id'))
            );

        $query = with(
            cte('org_tree', $orgTreeQuery, ['id', 'name', 'manager_id', 'level', 'path']),
            cte('dept_stats', $deptStats, ['department_id', 'avg_salary', 'max_salary', 'employee_count']),
            cte(
                'ranked_employees',
                $rankedEmployees,
                ['id', 'name', 'department_id', 'salary', 'salary_rank', 'pct_of_max']
            )
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
                    when(
                        binary_expr(col('re.salary'), '>', col('ds.avg_salary')),
                        literal('Above Average')
                    ),
                ], literal('At/Below Average'))->as('salary_status')
            )
            ->from(table('org_tree'))
            ->join(table('ranked_employees')->as('re'), eq(col('org_tree.id'), col('re.id')))
            ->join(
                table('dept_stats')->as('ds'),
                eq(col('re.department_id'), col('ds.department_id'))
            )
            ->join(table('departments')->as('d'), eq(col('re.department_id'), col('d.id')))
            ->where(cond_and(
                lte(col('org_tree.level'), literal(3)),
                lte(col('re.salary_rank'), literal(5))
            ))
            ->orderBy(
                asc(col('org_tree.level')),
                asc(col('d.name')),
                desc(col('re.salary'))
            );

        $expectedSql = <<<'SQL'
            WITH RECURSIVE org_tree(id, name, manager_id, level, path) AS (SELECT id, name, manager_id, 0 AS level, name::pg_catalog.text AS path FROM employees WHERE manager_id IS NULL UNION ALL SELECT e.id, e.name, e.manager_id, org_tree.level + 1, (org_tree.path || ' -> ') || e.name FROM employees e JOIN org_tree ON e.manager_id = org_tree.id), dept_stats(department_id, avg_salary, max_salary, employee_count) AS (SELECT department_id, avg(salary), max(salary), count(*) FROM employees GROUP BY department_id), ranked_employees(id, name, department_id, salary, salary_rank, pct_of_max) AS (SELECT e.id, e.name, e.department_id, e.salary, row_number() OVER (PARTITION BY e.department_id ORDER BY e.salary DESC), round((e.salary / ds.max_salary) * 100, 2) FROM employees e JOIN dept_stats ds ON e.department_id = ds.department_id) SELECT org_tree.name AS employee, org_tree.level, org_tree.path AS reporting_chain, d.name AS department, re.salary, re.salary_rank, re.pct_of_max, ds.avg_salary AS dept_avg, CASE WHEN re.salary > ds.avg_salary THEN 'Above Average' ELSE 'At/Below Average' END AS salary_status FROM org_tree JOIN ranked_employees re ON org_tree.id = re.id JOIN dept_stats ds ON re.department_id = ds.department_id JOIN departments d ON re.department_id = d.id WHERE org_tree.level <= 3 AND re.salary_rank <= 5 ORDER BY org_tree.level ASC, d.name ASC, re.salary DESC
            SQL;

        $this->assertSelectQueryRoundTrip($query, $expectedSql);
    }

    public function test_select_with_right_join() : void
    {
        $query = select()
            ->select(star())
            ->from(table('orders')->as('o'))
            ->rightJoin(
                table('users')->as('u'),
                eq(col('o.user_id'), col('u.id'))
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
                    row_expr([col('first_name'), col('last_name')]),
                    row_expr([literal('John'), literal('Doe')])
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
            ->where(eq(col('orders.user_id'), col('users.id')));

        $query = select()
            ->select(
                col('name'),
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
            ->where(similar_to(col('name'), literal('%John%')));

        $this->assertSelectQueryRoundTrip(
            $query,
            "SELECT * FROM users WHERE name SIMILAR TO '%John%'"
        );
    }

    public function test_select_with_table_function() : void
    {
        $query = select()
            ->select(col('value'))
            ->from(table_func(func('generate_series', [literal(1), literal(10)]))->as('value'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT value FROM generate_series(1, 10) value'
        );
    }

    public function test_select_with_type_cast() : void
    {
        $query = select()
            ->select(
                col('id'),
                cast(col('price'), data_type_integer())->as('price_int')
            )
            ->from(table('products'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, price::int AS price_int FROM products'
        );
    }

    public function test_select_with_union() : void
    {
        $query1 = select()
            ->select(col('name'))
            ->from(table('users'));

        $query2 = select()
            ->select(col('name'))
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
            ->select(col('id'))
            ->from(table('table1'));

        $query2 = select()
            ->select(col('id'))
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
                    gt(col('price'), literal(10)),
                    lt(col('price'), literal(100)),
                    gte(col('quantity'), literal(1)),
                    lte(col('weight'), literal(50))
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
                col('department'),
                col('salary'),
                window_func(
                    'row_number',
                    [],
                    [],
                    [order_by(col('salary'), SortDirection::DESC)]
                )->as('rank')
            )
            ->from(table('employees'))
            ->window(window_def(
                'w',
                [col('department')],
                [order_by(col('salary'), SortDirection::DESC)]
            ));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT department, salary, row_number() OVER (ORDER BY salary DESC) AS rank FROM employees WINDOW w AS (PARTITION BY department ORDER BY salary DESC)'
        );
    }

    public function test_simple_select() : void
    {
        $query = select()
            ->select(col('id'), col('name'))
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
            ->where(eq(col('active'), literal(true)));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT * FROM users WHERE active = true'
        );
    }

    public function test_sql_value_function_current_date() : void
    {
        $query = select()
            ->select(current_date()->as('today'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT current_date AS today'
        );
    }

    public function test_sql_value_function_current_time() : void
    {
        $query = select()
            ->select(current_time()->as('now_time'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT current_time AS now_time'
        );
    }

    public function test_sql_value_function_current_timestamp() : void
    {
        $query = select()
            ->select(current_timestamp()->as('now'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT current_timestamp AS now'
        );
    }

    public function test_sql_value_functions_with_other_columns() : void
    {
        $query = select()
            ->select(
                col('id'),
                col('name'),
                current_timestamp()->as('created_at')
            )
            ->from(table('users'));

        $this->assertSelectQueryRoundTrip(
            $query,
            'SELECT id, name, current_timestamp AS created_at FROM users'
        );
    }
}

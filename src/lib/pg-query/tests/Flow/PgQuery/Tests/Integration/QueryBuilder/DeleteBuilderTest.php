<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    pg_any,
    pg_col,
    pg_delete,
    pg_eq,
    pg_int,
    pg_param,
    pg_select,
    pg_table
};

use Flow\PgQuery\QueryBuilder\Condition\ComparisonOperator;

final class DeleteBuilderTest extends PGQueryTestCase
{
    public function test_delete_with_alias() : void
    {
        $query = pg_delete()
            ->from('users', 'u')
            ->where(pg_eq(pg_col('u.id'), pg_int(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users u WHERE u.id = 1'
        );
    }

    public function test_delete_with_parameters() : void
    {
        $query = pg_delete()
            ->from('users')
            ->where(pg_eq(pg_col('id'), pg_param(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = $1'
        );
    }

    public function test_delete_with_returning() : void
    {
        $query = pg_delete()
            ->from('users')
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->returning(pg_col('id'), pg_col('name'));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1 RETURNING id, name'
        );
    }

    public function test_delete_with_returning_all() : void
    {
        $query = pg_delete()
            ->from('users')
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->returningAll();

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1 RETURNING *'
        );
    }

    public function test_delete_with_subquery_in_where() : void
    {
        $subquery = pg_select()
            ->select(pg_col('user_id'))
            ->from(pg_table('inactive_users'));

        $query = pg_delete()
            ->from('users')
            ->where(pg_any(pg_col('id'), ComparisonOperator::EQ, $subquery));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = ANY (SELECT user_id FROM inactive_users)'
        );
    }

    public function test_delete_with_using() : void
    {
        $query = pg_delete()
            ->from('orders')
            ->using(pg_table('users'))
            ->where(pg_eq(pg_col('orders.user_id'), pg_col('users.id')));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM orders USING users WHERE orders.user_id = users.id'
        );
    }

    public function test_simple_delete() : void
    {
        $query = pg_delete()
            ->from('users')
            ->where(pg_eq(pg_col('id'), pg_int(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1'
        );
    }
}

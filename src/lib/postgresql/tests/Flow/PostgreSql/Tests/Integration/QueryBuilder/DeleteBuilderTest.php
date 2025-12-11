<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\QueryBuilder;

use function Flow\PostgreSql\DSL\{any_sub_select, col, delete, eq, literal, param, select, table};

use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;

final class DeleteBuilderTest extends PGQueryTestCase
{
    public function test_delete_with_alias() : void
    {
        $query = delete()
            ->from('users', 'u')
            ->where(eq(col('id', 'u'), literal(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users u WHERE u.id = 1'
        );
    }

    public function test_delete_with_parameters() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col('id'), param(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = $1'
        );
    }

    public function test_delete_with_returning() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col('id'), literal(1)))
            ->returning(col('id'), col('name'));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1 RETURNING id, name'
        );
    }

    public function test_delete_with_returning_all() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col('id'), literal(1)))
            ->returningAll();

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1 RETURNING *'
        );
    }

    public function test_delete_with_subquery_in_where() : void
    {
        $subquery = select()
            ->select(col('user_id'))
            ->from(table('inactive_users'));

        $query = delete()
            ->from('users')
            ->where(any_sub_select(col('id'), ComparisonOperator::EQ, $subquery));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = ANY (SELECT user_id FROM inactive_users)'
        );
    }

    public function test_delete_with_using() : void
    {
        $query = delete()
            ->from('orders')
            ->using(table('users'))
            ->where(eq(col('orders.user_id'), col('users.id')));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM orders USING users WHERE orders.user_id = users.id'
        );
    }

    public function test_simple_delete() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col('id'), literal(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1'
        );
    }
}

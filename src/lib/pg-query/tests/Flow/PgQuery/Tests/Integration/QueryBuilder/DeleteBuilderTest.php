<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    any_sub_select,
    col_from_string,
    delete,
    eq,
    literal_int,
    param,
    select,
    table
};

use Flow\PgQuery\QueryBuilder\Condition\ComparisonOperator;

final class DeleteBuilderTest extends PGQueryTestCase
{
    public function test_delete_with_alias() : void
    {
        $query = delete()
            ->from('users', 'u')
            ->where(eq(col_from_string('u.id'), literal_int(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users u WHERE u.id = 1'
        );
    }

    public function test_delete_with_parameters() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col_from_string('id'), param(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = $1'
        );
    }

    public function test_delete_with_returning() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col_from_string('id'), literal_int(1)))
            ->returning(col_from_string('id'), col_from_string('name'));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1 RETURNING id, name'
        );
    }

    public function test_delete_with_returning_all() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col_from_string('id'), literal_int(1)))
            ->returningAll();

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1 RETURNING *'
        );
    }

    public function test_delete_with_subquery_in_where() : void
    {
        $subquery = select()
            ->select(col_from_string('user_id'))
            ->from(table('inactive_users'));

        $query = delete()
            ->from('users')
            ->where(any_sub_select(col_from_string('id'), ComparisonOperator::EQ, $subquery));

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
            ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM orders USING users WHERE orders.user_id = users.id'
        );
    }

    public function test_simple_delete() : void
    {
        $query = delete()
            ->from('users')
            ->where(eq(col_from_string('id'), literal_int(1)));

        $this->assertDeleteQueryRoundTrip(
            $query,
            'DELETE FROM users WHERE id = 1'
        );
    }
}

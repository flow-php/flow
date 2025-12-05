<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    pg_col,
    pg_eq,
    pg_int,
    pg_param,
    pg_select,
    pg_string,
    pg_subquery,
    pg_table,
    pg_update
};

final class UpdateBuilderTest extends PGQueryTestCase
{
    public function test_simple_update() : void
    {
        $query = pg_update()
            ->update('users')
            ->set('name', pg_string('John'))
            ->where(pg_eq(pg_col('id'), pg_int(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John' WHERE id = 1"
        );
    }

    public function test_update_with_alias() : void
    {
        $query = pg_update()
            ->update('users', 'u')
            ->set('name', pg_string('John'))
            ->where(pg_eq(pg_col('u.id'), pg_int(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users u SET name = 'John' WHERE u.id = 1"
        );
    }

    public function test_update_with_column_expression() : void
    {
        $query = pg_update()
            ->update('products')
            ->set('price', pg_col('price'))
            ->where(pg_eq(pg_col('id'), pg_int(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            'UPDATE products SET price = price WHERE id = 1'
        );
    }

    public function test_update_with_from() : void
    {
        $query = pg_update()
            ->update('orders')
            ->set('status', pg_string('completed'))
            ->from(pg_table('users'))
            ->where(pg_eq(pg_col('orders.user_id'), pg_col('users.id')));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE orders SET status = 'completed' FROM users WHERE orders.user_id = users.id"
        );
    }

    public function test_update_with_multiple_set() : void
    {
        $query = pg_update()
            ->update('users')
            ->set('name', pg_string('John'))
            ->set('email', pg_string('john@example.com'))
            ->where(pg_eq(pg_col('id'), pg_int(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1"
        );
    }

    public function test_update_with_parameters() : void
    {
        $query = pg_update()
            ->update('users')
            ->set('name', pg_param(1))
            ->where(pg_eq(pg_col('id'), pg_param(2)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            'UPDATE users SET name = $1 WHERE id = $2'
        );
    }

    public function test_update_with_returning() : void
    {
        $query = pg_update()
            ->update('users')
            ->set('name', pg_string('John'))
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->returning(pg_col('id'), pg_col('name'));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John' WHERE id = 1 RETURNING id, name"
        );
    }

    public function test_update_with_returning_all() : void
    {
        $query = pg_update()
            ->update('users')
            ->set('name', pg_string('John'))
            ->where(pg_eq(pg_col('id'), pg_int(1)))
            ->returningAll();

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John' WHERE id = 1 RETURNING *"
        );
    }

    public function test_update_with_set_all() : void
    {
        $query = pg_update()
            ->update('users')
            ->setAll([
                'name' => pg_string('John'),
                'email' => pg_string('john@example.com'),
            ])
            ->where(pg_eq(pg_col('id'), pg_int(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1"
        );
    }

    public function test_update_with_subquery_in_set() : void
    {
        $subquery = pg_select()
            ->select(pg_col('avg_price'))
            ->from(pg_table('price_stats'))
            ->where(pg_eq(pg_col('category'), pg_col('products.category')));

        $query = pg_update()
            ->update('products')
            ->set('price', pg_subquery($subquery))
            ->where(pg_eq(pg_col('id'), pg_int(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            'UPDATE products SET price = (SELECT avg_price FROM price_stats WHERE category = products.category) WHERE id = 1'
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Integration\QueryBuilder;

use function Flow\PgQuery\DSL\{
    col,
    eq,
    literal,
    param,
    select,
    sub_select,
    table,
    update
};

final class UpdateBuilderTest extends PGQueryTestCase
{
    public function test_simple_update() : void
    {
        $query = update()
            ->update('users')
            ->set('name', literal('John'))
            ->where(eq(col('id'), literal(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John' WHERE id = 1"
        );
    }

    public function test_update_with_alias() : void
    {
        $query = update()
            ->update('users', 'u')
            ->set('name', literal('John'))
            ->where(eq(col('u.id'), literal(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users u SET name = 'John' WHERE u.id = 1"
        );
    }

    public function test_update_with_column_expression() : void
    {
        $query = update()
            ->update('products')
            ->set('price', col('price'))
            ->where(eq(col('id'), literal(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            'UPDATE products SET price = price WHERE id = 1'
        );
    }

    public function test_update_with_from() : void
    {
        $query = update()
            ->update('orders')
            ->set('status', literal('completed'))
            ->from(table('users'))
            ->where(eq(col('orders.user_id'), col('users.id')));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE orders SET status = 'completed' FROM users WHERE orders.user_id = users.id"
        );
    }

    public function test_update_with_multiple_set() : void
    {
        $query = update()
            ->update('users')
            ->set('name', literal('John'))
            ->set('email', literal('john@example.com'))
            ->where(eq(col('id'), literal(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1"
        );
    }

    public function test_update_with_parameters() : void
    {
        $query = update()
            ->update('users')
            ->set('name', param(1))
            ->where(eq(col('id'), param(2)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            'UPDATE users SET name = $1 WHERE id = $2'
        );
    }

    public function test_update_with_returning() : void
    {
        $query = update()
            ->update('users')
            ->set('name', literal('John'))
            ->where(eq(col('id'), literal(1)))
            ->returning(col('id'), col('name'));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John' WHERE id = 1 RETURNING id, name"
        );
    }

    public function test_update_with_returning_all() : void
    {
        $query = update()
            ->update('users')
            ->set('name', literal('John'))
            ->where(eq(col('id'), literal(1)))
            ->returningAll();

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John' WHERE id = 1 RETURNING *"
        );
    }

    public function test_update_with_set_all() : void
    {
        $query = update()
            ->update('users')
            ->setAll([
                'name' => literal('John'),
                'email' => literal('john@example.com'),
            ])
            ->where(eq(col('id'), literal(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            "UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1"
        );
    }

    public function test_update_with_subquery_in_set() : void
    {
        $subquery = select()
            ->select(col('avg_price'))
            ->from(table('price_stats'))
            ->where(eq(col('category'), col('products.category')));

        $query = update()
            ->update('products')
            ->set('price', sub_select($subquery))
            ->where(eq(col('id'), literal(1)));

        $this->assertUpdateQueryRoundTrip(
            $query,
            'UPDATE products SET price = (SELECT avg_price FROM price_stats WHERE category = products.category) WHERE id = 1'
        );
    }
}

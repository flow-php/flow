<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\DSL;

use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\agg_sum;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\delete;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\update;

final class StringCoercionTest extends TestCase
{
    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded...');
        }
    }

    public function test_agg_sum_with_string_in_select(): void
    {
        static::assertSame(
            select(agg_sum(col('amount')))->from(table('orders'))->toSql(),
            select(agg_sum('amount'))->from('orders')->toSql(),
        );
    }

    public function test_asc_with_string_in_order_by(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->orderBy(asc(col('created_at')))
                ->toSql(),
            select(star())->from('users')->orderBy(asc('created_at'))->toSql(),
        );
    }

    public function test_between_with_string_in_where(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->where(\Flow\PostgreSql\DSL\between(col('age'), literal(18), literal(65)))
                ->toSql(),
            select(star())
                ->from('users')
                ->where(\Flow\PostgreSql\DSL\between('age', literal(18), literal(65)))
                ->toSql(),
        );
    }

    public function test_cast_with_string_in_select(): void
    {
        static::assertSame(
            select(\Flow\PostgreSql\DSL\cast(col('id'), \Flow\PostgreSql\QueryBuilder\Schema\ColumnType::integer()))
                ->from(table('users'))
                ->toSql(),
            select(\Flow\PostgreSql\DSL\cast('id', \Flow\PostgreSql\QueryBuilder\Schema\ColumnType::integer()))
                ->from('users')
                ->toSql(),
        );
    }

    public function test_coalesce_with_mixed_strings_in_select(): void
    {
        static::assertSame(
            select(\Flow\PostgreSql\DSL\coalesce(col('name'), literal('unknown')))->from(table('users'))->toSql(),
            select(\Flow\PostgreSql\DSL\coalesce('name', literal('unknown')))->from('users')->toSql(),
        );
    }

    public function test_delete_using_with_string(): void
    {
        static::assertSame(
            delete()
                ->from('users')
                ->using(table('orders'))
                ->where(eq(col('users.id'), col('orders.user_id')))
                ->toSql(),
            delete()->from('users')->using(table('orders'))->where(eq('users.id', 'orders.user_id'))->toSql(),
        );
    }

    public function test_eq_with_strings_in_where(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->where(eq(col('status'), col('other')))
                ->toSql(),
            select(star())->from('users')->where(eq('status', 'other'))->toSql(),
        );
    }

    public function test_from_with_schema_dot_table(): void
    {
        static::assertSame(
            select(star())->from(table('users', 'public'))->toSql(),
            select(star())->from('public.users')->toSql(),
        );
    }

    public function test_from_with_string(): void
    {
        static::assertSame(select(star())->from(table('users'))->toSql(), select(star())->from('users')->toSql());
    }

    public function test_full_query_with_string_coercion(): void
    {
        static::assertSame(
            select(col('id'), col('name'), agg_sum(col('amount'))->as('total'))
                ->from(table('orders'))
                ->join(table('users'), eq(col('orders.user_id'), col('users.id')))
                ->where(eq(col('status'), literal('active')))
                ->groupBy(col('id'), col('name'))
                ->orderBy(asc(col('name')))
                ->toSql(),
            select('id', 'name', agg_sum('amount')->as('total'))
                ->from('orders')
                ->join('users', eq('orders.user_id', 'users.id'))
                ->where(eq('status', literal('active')))
                ->groupBy('id', 'name')
                ->orderBy(asc('name'))
                ->toSql(),
        );
    }

    public function test_group_by_with_strings(): void
    {
        static::assertSame(
            select(star())->from(table('users'))->groupBy(col('category'), col('status'))->toSql(),
            select(star())->from('users')->groupBy('category', 'status')->toSql(),
        );
    }

    public function test_is_null_with_string_in_where(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->where(\Flow\PostgreSql\DSL\is_null(col('email')))
                ->toSql(),
            select(star())
                ->from('users')
                ->where(\Flow\PostgreSql\DSL\is_null('email'))
                ->toSql(),
        );
    }

    public function test_join_with_string_table(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->join(table('orders'), eq(col('users.id'), col('orders.user_id')))
                ->toSql(),
            select(star())->from('users')->join('orders', eq('users.id', 'orders.user_id'))->toSql(),
        );
    }

    public function test_left_join_with_string_table(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->leftJoin(table('orders'), eq(col('users.id'), col('orders.user_id')))
                ->toSql(),
            select(star())->from('users')->leftJoin('orders', eq('users.id', 'orders.user_id'))->toSql(),
        );
    }

    public function test_like_with_string_in_where(): void
    {
        static::assertSame(
            select(star())
                ->from(table('users'))
                ->where(\Flow\PostgreSql\DSL\like(col('name'), literal('%test%')))
                ->toSql(),
            select(star())
                ->from('users')
                ->where(\Flow\PostgreSql\DSL\like('name', literal('%test%')))
                ->toSql(),
        );
    }

    public function test_nullif_with_strings_in_select(): void
    {
        static::assertSame(
            select(\Flow\PostgreSql\DSL\nullif(col('a'), col('b')))->from(table('users'))->toSql(),
            select(\Flow\PostgreSql\DSL\nullif('a', 'b'))->from('users')->toSql(),
        );
    }

    public function test_returning_with_strings_in_delete(): void
    {
        static::assertSame(
            delete()
                ->from('users')
                ->where(eq(col('id'), literal(1)))
                ->returning(col('id'), col('name'))
                ->toSql(),
            delete()
                ->from('users')
                ->where(eq('id', literal(1)))
                ->returning(col('id'), col('name'))
                ->toSql(),
        );
    }

    public function test_select_mixed_strings_and_expressions(): void
    {
        static::assertSame(
            select(col('id'), col('name')->as('n'))->from(table('users'))->toSql(),
            select('id', col('name')->as('n'))->from(table('users'))->toSql(),
        );
    }

    public function test_select_with_strings_produces_same_sql_as_col(): void
    {
        static::assertSame(
            select(col('id'), col('name'))->from(table('users'))->toSql(),
            select('id', 'name')->from(table('users'))->toSql(),
        );
    }

    public function test_update_from_with_string(): void
    {
        static::assertSame(
            update()
                ->update('users')
                ->set('name', literal('test'))
                ->from(table('logs'))
                ->where(eq(col('users.id'), col('logs.user_id')))
                ->toSql(),
            update()
                ->update('users')
                ->set('name', literal('test'))
                ->from(table('logs'))
                ->where(eq('users.id', 'logs.user_id'))
                ->toSql(),
        );
    }
}

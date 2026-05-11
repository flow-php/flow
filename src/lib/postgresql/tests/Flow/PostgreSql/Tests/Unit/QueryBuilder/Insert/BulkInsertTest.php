<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\QueryBuilder\Insert;

use Flow\PostgreSql\QueryBuilder\Clause\ConflictTarget;
use Flow\PostgreSql\QueryBuilder\Insert\BulkInsert;
use PHPUnit\Framework\TestCase;

final class BulkInsertTest extends TestCase
{
    public function test_basic_insert_multiple_rows(): void
    {
        $query = BulkInsert::into('users', ['name', 'email'], 3);

        static::assertSame(
            'INSERT INTO "users" ("name", "email") VALUES ($1, $2), ($3, $4), ($5, $6)',
            $query->toSql(),
        );
    }

    public function test_basic_insert_single_row(): void
    {
        $query = BulkInsert::into('users', ['name', 'email'], 1);

        static::assertSame('INSERT INTO "users" ("name", "email") VALUES ($1, $2)', $query->toSql());
    }

    public function test_immutability_on_conflict_do_nothing(): void
    {
        $query1 = BulkInsert::into('users', ['name'], 1);
        $query2 = $query1->onConflictDoNothing();

        static::assertNotSame($query1, $query2);
        static::assertStringNotContainsString('ON CONFLICT', $query1->toSql());
        static::assertStringContainsString('ON CONFLICT', $query2->toSql());
    }

    public function test_immutability_on_conflict_do_update(): void
    {
        $query1 = BulkInsert::into('users', ['email', 'name'], 1);
        $query2 = $query1->onConflictDoUpdate(ConflictTarget::columns(['email']));

        static::assertNotSame($query1, $query2);
        static::assertStringNotContainsString('ON CONFLICT', $query1->toSql());
        static::assertStringContainsString('DO UPDATE', $query2->toSql());
    }

    public function test_insert_with_many_columns(): void
    {
        $query = BulkInsert::into('products', ['id', 'name', 'price', 'category', 'stock'], 2);

        static::assertSame(
            'INSERT INTO "products" ("id", "name", "price", "category", "stock") VALUES ($1, $2, $3, $4, $5), ($6, $7, $8, $9, $10)',
            $query->toSql(),
        );
    }

    public function test_large_batch_placeholder_generation(): void
    {
        $query = BulkInsert::into('data', ['a', 'b', 'c'], 100);
        $sql = $query->toSql();

        static::assertStringContainsString('$1, $2, $3', $sql);
        static::assertStringContainsString('$298, $299, $300', $sql);
        static::assertStringNotContainsString('$301', $sql);
    }

    public function test_on_conflict_do_nothing_with_columns(): void
    {
        $query = BulkInsert::into('users', ['name', 'email'], 1)->onConflictDoNothing(ConflictTarget::columns([
            'email',
        ]));

        static::assertSame(
            'INSERT INTO "users" ("name", "email") VALUES ($1, $2) ON CONFLICT ("email") DO NOTHING',
            $query->toSql(),
        );
    }

    public function test_on_conflict_do_nothing_with_constraint(): void
    {
        $query = BulkInsert::into('users', ['name', 'email'], 1)->onConflictDoNothing(ConflictTarget::constraint(
            'users_email_key',
        ));

        static::assertSame(
            'INSERT INTO "users" ("name", "email") VALUES ($1, $2) ON CONFLICT ON CONSTRAINT "users_email_key" DO NOTHING',
            $query->toSql(),
        );
    }

    public function test_on_conflict_do_nothing_without_target(): void
    {
        $query = BulkInsert::into('users', ['name', 'email'], 1)->onConflictDoNothing();

        static::assertSame(
            'INSERT INTO "users" ("name", "email") VALUES ($1, $2) ON CONFLICT DO NOTHING',
            $query->toSql(),
        );
    }

    public function test_on_conflict_do_update_with_all_non_conflict_columns(): void
    {
        $query = BulkInsert::into('users', ['email', 'name', 'age'], 1)->onConflictDoUpdate(ConflictTarget::columns([
            'email',
        ]));

        static::assertSame(
            'INSERT INTO "users" ("email", "name", "age") VALUES ($1, $2, $3) ON CONFLICT ("email") DO UPDATE SET "name" = EXCLUDED."name", "age" = EXCLUDED."age"',
            $query->toSql(),
        );
    }

    public function test_on_conflict_do_update_with_constraint(): void
    {
        $query = BulkInsert::into('users', ['email', 'name'], 1)->onConflictDoUpdate(
            ConflictTarget::constraint('users_pkey'),
            ['name'],
        );

        static::assertSame(
            'INSERT INTO "users" ("email", "name") VALUES ($1, $2) ON CONFLICT ON CONSTRAINT "users_pkey" DO UPDATE SET "name" = EXCLUDED."name"',
            $query->toSql(),
        );
    }

    public function test_on_conflict_do_update_with_explicit_columns(): void
    {
        $query = BulkInsert::into('users', ['email', 'name', 'age'], 1)->onConflictDoUpdate(
            ConflictTarget::columns(['email']),
            ['name', 'age'],
        );

        static::assertSame(
            'INSERT INTO "users" ("email", "name", "age") VALUES ($1, $2, $3) ON CONFLICT ("email") DO UPDATE SET "name" = EXCLUDED."name", "age" = EXCLUDED."age"',
            $query->toSql(),
        );
    }

    public function test_on_conflict_do_update_with_multiple_conflict_columns(): void
    {
        $query = BulkInsert::into('orders', ['customer_id', 'product_id', 'quantity'], 1)->onConflictDoUpdate(
            ConflictTarget::columns(['customer_id', 'product_id']),
            ['quantity'],
        );

        static::assertSame(
            'INSERT INTO "orders" ("customer_id", "product_id", "quantity") VALUES ($1, $2, $3) ON CONFLICT ("customer_id", "product_id") DO UPDATE SET "quantity" = EXCLUDED."quantity"',
            $query->toSql(),
        );
    }

    public function test_placeholder_offset_calculation(): void
    {
        $query = BulkInsert::into('test', ['col1', 'col2'], 3);
        $sql = $query->toSql();

        static::assertStringContainsString('($1, $2)', $sql);
        static::assertStringContainsString('($3, $4)', $sql);
        static::assertStringContainsString('($5, $6)', $sql);
    }

    public function test_throws_exception_for_empty_columns(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('At least one column is required');

        BulkInsert::into('users', [], 1);
    }

    public function test_throws_exception_for_negative_rows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Row count must be at least 1');

        BulkInsert::into('users', ['name'], -1);
    }

    public function test_throws_exception_for_zero_rows(): void
    {
        $this->expectException(\InvalidArgumentException::class);
        $this->expectExceptionMessage('Row count must be at least 1');

        BulkInsert::into('users', ['name'], 0);
    }
}

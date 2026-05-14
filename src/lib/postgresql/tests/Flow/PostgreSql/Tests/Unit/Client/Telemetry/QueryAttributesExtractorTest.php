<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\Client\Telemetry;

use Flow\PostgreSql\Client\Telemetry\QueryAttributesExtractor;
use PHPUnit\Framework\TestCase;

final class QueryAttributesExtractorTest extends TestCase
{
    public function test_extract_both_operation_and_target(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('SELECT * FROM users WHERE id = 1');

        static::assertSame('SELECT', $result->operation);
        static::assertSame('users', $result->target);
    }

    public function test_extract_delete_from_table(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('DELETE FROM users WHERE id = 1');

        static::assertSame('DELETE', $result->operation);
        static::assertSame('users', $result->target);
    }

    public function test_extract_insert_into_table(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('INSERT INTO users (name) VALUES ($1)');

        static::assertSame('INSERT', $result->operation);
        static::assertSame('users', $result->target);
    }

    public function test_extract_null_operation_for_unknown_query(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('VACUUM users');

        static::assertNull($result->operation);
        static::assertNull($result->target);
    }

    public function test_extract_null_target_for_query_without_table(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('SELECT 1');

        static::assertSame('SELECT', $result->operation);
        static::assertNull($result->target);
    }

    public function test_extract_operation_alter(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('ALTER', $extractor->extract('ALTER TABLE users ADD COLUMN email VARCHAR(255)')->operation);
    }

    public function test_extract_operation_begin(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('BEGIN', $extractor->extract('BEGIN')->operation);
    }

    public function test_extract_operation_commit(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('COMMIT', $extractor->extract('COMMIT')->operation);
    }

    public function test_extract_operation_copy(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('COPY', $extractor->extract('COPY users TO stdout')->operation);
    }

    public function test_extract_operation_create(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('CREATE', $extractor->extract('CREATE TABLE users (id SERIAL PRIMARY KEY)')->operation);
    }

    public function test_extract_operation_delete(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('DELETE', $extractor->extract('DELETE FROM users WHERE id = 1')->operation);
    }

    public function test_extract_operation_drop(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('DROP', $extractor->extract('DROP TABLE users')->operation);
    }

    public function test_extract_operation_explain(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('EXPLAIN', $extractor->extract('EXPLAIN ANALYZE SELECT * FROM users')->operation);
    }

    public function test_extract_operation_from_case_insensitive_query(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('SELECT', $extractor->extract('select * FROM users')->operation);
    }

    public function test_extract_operation_insert(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('INSERT', $extractor->extract('INSERT INTO users (name) VALUES ($1)')->operation);
    }

    public function test_extract_operation_merge(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame(
            'MERGE',
            $extractor->extract('MERGE INTO users USING source ON users.id = source.id')->operation,
        );
    }

    public function test_extract_operation_returns_null_for_unknown_operation(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertNull($extractor->extract('VACUUM users')->operation);
    }

    public function test_extract_operation_rollback(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('ROLLBACK', $extractor->extract('ROLLBACK')->operation);
    }

    public function test_extract_operation_select(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('SELECT', $extractor->extract('SELECT * FROM users')->operation);
    }

    public function test_extract_operation_truncate(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('TRUNCATE', $extractor->extract('TRUNCATE TABLE users')->operation);
    }

    public function test_extract_operation_update(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('UPDATE', $extractor->extract('UPDATE users SET name = $1 WHERE id = $2')->operation);
    }

    public function test_extract_operation_with(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame(
            'WITH',
            $extractor->extract('WITH cte AS (SELECT * FROM users) SELECT * FROM cte')->operation,
        );
    }

    public function test_extract_operation_with_leading_whitespace(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('SELECT', $extractor->extract('  SELECT * FROM users')->operation);
    }

    public function test_extract_schema_qualified_table(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('SELECT * FROM public.users');

        static::assertSame('SELECT', $result->operation);
        static::assertSame('public.users', $result->target);
    }

    public function test_extract_target_finds_first_from_clause(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('orders', $extractor->extract('SELECT * FROM orders o')->target);
    }

    public function test_extract_target_from_delete(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('users', $extractor->extract('DELETE FROM users WHERE id = 1')->target);
    }

    public function test_extract_target_from_insert(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('users', $extractor->extract('INSERT INTO users (name) VALUES ($1)')->target);
    }

    public function test_extract_target_from_quoted_table(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('Users', $extractor->extract('SELECT * FROM "Users"')->target);
    }

    public function test_extract_target_from_schema_qualified_table(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('public.users', $extractor->extract('SELECT * FROM public.users')->target);
    }

    public function test_extract_target_from_select(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('users', $extractor->extract('SELECT * FROM users WHERE id = 1')->target);
    }

    public function test_extract_target_from_update(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame('users', $extractor->extract('UPDATE users SET name = $1 WHERE id = $2')->target);
    }

    public function test_extract_target_prefers_from_over_join(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertSame(
            'orders',
            $extractor->extract('SELECT * FROM orders JOIN users ON orders.user_id = users.id')->target,
        );
    }

    public function test_extract_target_returns_null_for_query_without_table(): void
    {
        $extractor = new QueryAttributesExtractor();

        static::assertNull($extractor->extract('SELECT 1')->target);
    }

    public function test_extract_update_table(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract('UPDATE users SET name = $1 WHERE id = $2');

        static::assertSame('UPDATE', $result->operation);
        static::assertSame('users', $result->target);
    }

    public function test_extract_with_multiline_query(): void
    {
        $extractor = new QueryAttributesExtractor();
        $result = $extractor->extract("SELECT *\n  FROM users\n  WHERE id = 1");

        static::assertSame('SELECT', $result->operation);
        static::assertSame('users', $result->target);
    }
}

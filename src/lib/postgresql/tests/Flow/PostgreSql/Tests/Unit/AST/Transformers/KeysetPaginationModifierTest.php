<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use function Flow\PostgreSql\DSL\{sql_keyset_column, sql_parse};
use Flow\PostgreSql\AST\Transformers\{KeysetPaginationConfig, KeysetPaginationModifier, SortOrder};
use Flow\PostgreSql\Exception\PaginationException;
use PHPUnit\Framework\TestCase;

final class KeysetPaginationModifierTest extends TestCase
{
    protected function setUp() : void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped('pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.');
        }
    }

    public function test_first_page_multiple_columns() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY created_at, id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [
            sql_keyset_column('created_at', SortOrder::ASC),
            sql_keyset_column('id', SortOrder::ASC),
        ]));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY created_at, id LIMIT 10', $parsed->deparse());
    }

    public function test_first_page_single_column_asc() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)]));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY id LIMIT 10', $parsed->deparse());
    }

    public function test_first_page_single_column_desc() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id DESC');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::DESC)]));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY id DESC LIMIT 10', $parsed->deparse());
    }

    public function test_keyset_pagination_config_default_cursor() : void
    {
        $columns = [sql_keyset_column('id', SortOrder::ASC)];

        $config = new KeysetPaginationConfig(20, $columns);

        self::assertSame(20, $config->limit);
        self::assertSame($columns, $config->columns);
        self::assertNull($config->cursor);
    }

    public function test_keyset_pagination_config_returns_config_object() : void
    {
        $columns = [
            sql_keyset_column('created_at', SortOrder::ASC),
            sql_keyset_column('id', SortOrder::ASC),
        ];
        $cursor = ['2025-01-15', 42];

        $config = new KeysetPaginationConfig(10, $columns, $cursor);

        self::assertSame(10, $config->limit);
        self::assertSame($columns, $config->columns);
        self::assertSame($cursor, $config->cursor);
    }

    public function test_qualified_column_names() : void
    {
        $parsed = sql_parse('SELECT u.* FROM users u ORDER BY u.id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('u.id', SortOrder::ASC)], [42]));
        $parsed->traverse($modifier);

        self::assertSame('SELECT u.* FROM users u WHERE u.id > $1 ORDER BY u.id LIMIT 10', $parsed->deparse());
    }

    public function test_subquery_not_modified() : void
    {
        $parsed = sql_parse('SELECT * FROM (SELECT id FROM users ORDER BY id) AS sub ORDER BY id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]));
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM (SELECT id FROM users ORDER BY id) sub WHERE id > $1 ORDER BY id LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_subsequent_page_multiple_columns_all_asc() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY created_at, id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [
            sql_keyset_column('created_at', SortOrder::ASC),
            sql_keyset_column('id', SortOrder::ASC),
        ], ['2025-01-15 12:30:00', 42]));
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id > $2) ORDER BY created_at, id LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_subsequent_page_multiple_columns_mixed_order() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY created_at ASC, id DESC');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [
            sql_keyset_column('created_at', SortOrder::ASC),
            sql_keyset_column('id', SortOrder::DESC),
        ], ['2025-01-15 12:30:00', 42]));
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id < $2) ORDER BY created_at ASC, id DESC LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_subsequent_page_single_column_asc() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users WHERE id > $1 ORDER BY id LIMIT 10', $parsed->deparse());
    }

    public function test_subsequent_page_single_column_desc() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY id DESC');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::DESC)], [42]));
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users WHERE id < $1 ORDER BY id DESC LIMIT 10', $parsed->deparse());
    }

    public function test_subsequent_page_three_columns() : void
    {
        $parsed = sql_parse('SELECT * FROM users ORDER BY status, created_at DESC, id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [
            sql_keyset_column('status', SortOrder::ASC),
            sql_keyset_column('created_at', SortOrder::DESC),
            sql_keyset_column('id', SortOrder::ASC),
        ], ['active', '2025-01-15', 100]));
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE status > $1 OR (status = $1 AND created_at < $2) OR (status = $1 AND created_at = $2 AND id > $3) ORDER BY status, created_at DESC, id LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_throws_when_cursor_count_mismatch() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Cursor values count (1) must match columns count (2)');

        $parsed = sql_parse('SELECT * FROM users ORDER BY created_at, id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [
            sql_keyset_column('created_at', SortOrder::ASC),
            sql_keyset_column('id', SortOrder::ASC),
        ], [42]));
        $parsed->traverse($modifier);
    }

    public function test_throws_when_no_columns() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Keyset pagination requires at least one column');

        $parsed = sql_parse('SELECT * FROM users ORDER BY id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, []));
        $parsed->traverse($modifier);
    }

    public function test_throws_when_no_order_by() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Keyset pagination requires ORDER BY clause');

        $parsed = sql_parse('SELECT * FROM users');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)]));
        $parsed->traverse($modifier);
    }

    public function test_with_existing_where_clause() : void
    {
        $parsed = sql_parse('SELECT * FROM users WHERE active = true ORDER BY id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]));
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE active = true AND id > $1 ORDER BY id LIMIT 10',
            $parsed->deparse()
        );
    }
}

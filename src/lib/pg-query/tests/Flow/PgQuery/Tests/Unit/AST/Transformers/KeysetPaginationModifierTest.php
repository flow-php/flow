<?php

declare(strict_types=1);

namespace Flow\PgQuery\Tests\Unit\AST\Transformers;

use Flow\PgQuery\AST\Transformers\{KeysetColumn, KeysetPaginationConfig, KeysetPaginationModifier, SortOrder};
use Flow\PgQuery\Exception\PaginationException;
use Flow\PgQuery\Parser;
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
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY created_at, id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [
                new KeysetColumn('created_at', SortOrder::ASC),
                new KeysetColumn('id', SortOrder::ASC),
            ],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY created_at, id LIMIT 10', $parsed->deparse());
    }

    public function test_first_page_single_column_asc() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::ASC)],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY id LIMIT 10', $parsed->deparse());
    }

    public function test_first_page_single_column_desc() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY id DESC');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::DESC)],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users ORDER BY id DESC LIMIT 10', $parsed->deparse());
    }

    public function test_qualified_column_names() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT u.* FROM users u ORDER BY u.id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('u.id', SortOrder::ASC)],
            cursor: [42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame('SELECT u.* FROM users u WHERE u.id > $1 ORDER BY u.id LIMIT 10', $parsed->deparse());
    }

    public function test_subquery_not_modified() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM (SELECT id FROM users ORDER BY id) AS sub ORDER BY id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::ASC)],
            cursor: [42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM (SELECT id FROM users ORDER BY id) sub WHERE id > $1 ORDER BY id LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_subsequent_page_multiple_columns_all_asc() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY created_at, id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [
                new KeysetColumn('created_at', SortOrder::ASC),
                new KeysetColumn('id', SortOrder::ASC),
            ],
            cursor: ['2025-01-15 12:30:00', 42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id > $2) ORDER BY created_at, id LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_subsequent_page_multiple_columns_mixed_order() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY created_at ASC, id DESC');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [
                new KeysetColumn('created_at', SortOrder::ASC),
                new KeysetColumn('id', SortOrder::DESC),
            ],
            cursor: ['2025-01-15 12:30:00', 42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id < $2) ORDER BY created_at ASC, id DESC LIMIT 10',
            $parsed->deparse()
        );
    }

    public function test_subsequent_page_single_column_asc() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::ASC)],
            cursor: [42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users WHERE id > $1 ORDER BY id LIMIT 10', $parsed->deparse());
    }

    public function test_subsequent_page_single_column_desc() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY id DESC');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::DESC)],
            cursor: [42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame('SELECT * FROM users WHERE id < $1 ORDER BY id DESC LIMIT 10', $parsed->deparse());
    }

    public function test_subsequent_page_three_columns() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY status, created_at DESC, id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [
                new KeysetColumn('status', SortOrder::ASC),
                new KeysetColumn('created_at', SortOrder::DESC),
                new KeysetColumn('id', SortOrder::ASC),
            ],
            cursor: ['active', '2025-01-15', 100],
        );
        $modifier = new KeysetPaginationModifier($config);
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

        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY created_at, id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [
                new KeysetColumn('created_at', SortOrder::ASC),
                new KeysetColumn('id', SortOrder::ASC),
            ],
            cursor: [42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);
    }

    public function test_throws_when_no_columns() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Keyset pagination requires at least one column');

        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users ORDER BY id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);
    }

    public function test_throws_when_no_order_by() : void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Keyset pagination requires ORDER BY clause');

        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::ASC)],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);
    }

    public function test_with_existing_where_clause() : void
    {
        $parser = new Parser();
        $parsed = $parser->parse('SELECT * FROM users WHERE active = true ORDER BY id');

        $config = new KeysetPaginationConfig(
            limit: 10,
            columns: [new KeysetColumn('id', SortOrder::ASC)],
            cursor: [42],
        );
        $modifier = new KeysetPaginationModifier($config);
        $parsed->traverse($modifier);

        self::assertSame(
            'SELECT * FROM users WHERE active = true AND id > $1 ORDER BY id LIMIT 10',
            $parsed->deparse()
        );
    }
}

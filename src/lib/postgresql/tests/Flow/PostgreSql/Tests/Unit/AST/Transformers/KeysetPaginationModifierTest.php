<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\KeysetPaginationConfig;
use Flow\PostgreSql\AST\Transformers\KeysetPaginationModifier;
use Flow\PostgreSql\AST\Transformers\SortOrder;
use Flow\PostgreSql\Exception\PaginationException;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\PostgreSql\DSL\sql_keyset_column;
use function Flow\PostgreSql\DSL\sql_parse;

final class KeysetPaginationModifierTest extends TestCase
{
    public static function keysetPaginationProvider(): \Generator
    {
        yield 'first page single column asc - no cursor' => [
            'SELECT * FROM users ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)]),
            'SELECT * FROM users ORDER BY id LIMIT 10',
        ];

        yield 'first page single column desc - no cursor' => [
            'SELECT * FROM users ORDER BY id DESC',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::DESC)]),
            'SELECT * FROM users ORDER BY id DESC LIMIT 10',
        ];

        yield 'first page multiple columns - no cursor' => [
            'SELECT * FROM users ORDER BY created_at, id',
            new KeysetPaginationConfig(10, [
                sql_keyset_column('created_at', SortOrder::ASC),
                sql_keyset_column('id', SortOrder::ASC),
            ]),
            'SELECT * FROM users ORDER BY created_at, id LIMIT 10',
        ];

        yield 'subsequent page single column asc - with cursor' => [
            'SELECT * FROM users ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]),
            'SELECT * FROM users WHERE id > $1 ORDER BY id LIMIT 10',
        ];

        yield 'subsequent page single column desc - with cursor' => [
            'SELECT * FROM users ORDER BY id DESC',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::DESC)], [42]),
            'SELECT * FROM users WHERE id < $1 ORDER BY id DESC LIMIT 10',
        ];

        yield 'subsequent page multiple columns all asc' => [
            'SELECT * FROM users ORDER BY created_at, id',
            new KeysetPaginationConfig(
                10,
                [
                    sql_keyset_column('created_at', SortOrder::ASC),
                    sql_keyset_column('id', SortOrder::ASC),
                ],
                ['2025-01-15 12:30:00', 42],
            ),
            'SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id > $2) ORDER BY created_at, id LIMIT 10',
        ];

        yield 'subsequent page multiple columns mixed order' => [
            'SELECT * FROM users ORDER BY created_at ASC, id DESC',
            new KeysetPaginationConfig(
                10,
                [
                    sql_keyset_column('created_at', SortOrder::ASC),
                    sql_keyset_column('id', SortOrder::DESC),
                ],
                ['2025-01-15 12:30:00', 42],
            ),
            'SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id < $2) ORDER BY created_at ASC, id DESC LIMIT 10',
        ];

        yield 'subsequent page three columns' => [
            'SELECT * FROM users ORDER BY status, created_at DESC, id',
            new KeysetPaginationConfig(
                10,
                [
                    sql_keyset_column('status', SortOrder::ASC),
                    sql_keyset_column('created_at', SortOrder::DESC),
                    sql_keyset_column('id', SortOrder::ASC),
                ],
                ['active', '2025-01-15', 100],
            ),
            'SELECT * FROM users WHERE status > $1 OR (status = $1 AND created_at < $2) OR (status = $1 AND created_at = $2 AND id > $3) ORDER BY status, created_at DESC, id LIMIT 10',
        ];

        yield 'with existing where clause' => [
            'SELECT * FROM users WHERE active = true ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]),
            'SELECT * FROM users WHERE active = true AND id > $1 ORDER BY id LIMIT 10',
        ];

        yield 'qualified column names' => [
            'SELECT u.* FROM users u ORDER BY u.id',
            new KeysetPaginationConfig(10, [sql_keyset_column('u.id', SortOrder::ASC)], [42]),
            'SELECT u.* FROM users u WHERE u.id > $1 ORDER BY u.id LIMIT 10',
        ];

        yield 'subquery not modified' => [
            'SELECT * FROM (SELECT id FROM users ORDER BY id) AS sub ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]),
            'SELECT * FROM (SELECT id FROM users ORDER BY id) sub WHERE id > $1 ORDER BY id LIMIT 10',
        ];

        yield 'auto generates order by when missing' => [
            'SELECT * FROM users',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)]),
            'SELECT * FROM users ORDER BY id ASC LIMIT 10',
        ];

        yield 'auto generates order by with cursor' => [
            'SELECT * FROM users',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]),
            'SELECT * FROM users WHERE id > $1 ORDER BY id ASC LIMIT 10',
        ];

        yield 'auto generates order by with multiple columns' => [
            'SELECT * FROM users',
            new KeysetPaginationConfig(10, [
                sql_keyset_column('created_at', SortOrder::DESC),
                sql_keyset_column('id', SortOrder::ASC),
            ]),
            'SELECT * FROM users ORDER BY created_at DESC, id ASC LIMIT 10',
        ];

        yield 'with existing parameters - offset auto-calculated' => [
            'SELECT * FROM users WHERE status = $1 ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]),
            'SELECT * FROM users WHERE status = $1 AND id > $2 ORDER BY id LIMIT 10',
        ];

        yield 'with multiple existing parameters - offset auto-calculated' => [
            'SELECT * FROM users WHERE status = $1 AND category = $2 ORDER BY created_at, id',
            new KeysetPaginationConfig(
                10,
                [
                    sql_keyset_column('created_at', SortOrder::ASC),
                    sql_keyset_column('id', SortOrder::ASC),
                ],
                ['2025-01-15', 42],
            ),
            'SELECT * FROM users WHERE (status = $1 AND category = $2) AND (created_at > $3 OR (created_at = $3 AND id > $4)) ORDER BY created_at, id LIMIT 10',
        ];

        yield 'with existing parameters - first page no cursor' => [
            'SELECT * FROM users WHERE status = $1 ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], null),
            'SELECT * FROM users WHERE status = $1 ORDER BY id LIMIT 10',
        ];

        yield 'with out-of-order parameters - max detected correctly' => [
            'SELECT * FROM users WHERE id > $10 OR status = $1 ORDER BY id',
            new KeysetPaginationConfig(10, [sql_keyset_column('id', SortOrder::ASC)], [42]),
            'SELECT * FROM users WHERE (id > $10 OR status = $1) AND id > $11 ORDER BY id LIMIT 10',
        ];
    }

    protected function setUp(): void
    {
        if (!\extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    #[DataProvider('keysetPaginationProvider')]
    public function test_keyset_pagination(string $inputSql, KeysetPaginationConfig $config, string $expectedSql): void
    {
        $parsed = sql_parse($inputSql);
        $parsed->traverse(new KeysetPaginationModifier($config));

        static::assertSame($expectedSql, $parsed->deparse());
    }

    public function test_keyset_pagination_config_default_cursor(): void
    {
        $columns = [sql_keyset_column('id', SortOrder::ASC)];

        $config = new KeysetPaginationConfig(20, $columns);

        static::assertSame(20, $config->limit);
        static::assertSame($columns, $config->columns);
        static::assertNull($config->cursor);
    }

    public function test_keyset_pagination_config_returns_config_object(): void
    {
        $columns = [
            sql_keyset_column('created_at', SortOrder::ASC),
            sql_keyset_column('id', SortOrder::ASC),
        ];
        $cursor = ['2025-01-15', 42];

        $config = new KeysetPaginationConfig(10, $columns, $cursor);

        static::assertSame(10, $config->limit);
        static::assertSame($columns, $config->columns);
        static::assertSame($cursor, $config->cursor);
    }

    public function test_throws_when_cursor_count_mismatch(): void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Cursor values count (1) must match columns count (2)');

        $parsed = sql_parse('SELECT * FROM users ORDER BY created_at, id');

        $modifier = new KeysetPaginationModifier(
            new KeysetPaginationConfig(
                10,
                [
                    sql_keyset_column('created_at', SortOrder::ASC),
                    sql_keyset_column('id', SortOrder::ASC),
                ],
                [42],
            ),
        );
        $parsed->traverse($modifier);
    }

    public function test_throws_when_no_columns(): void
    {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage('Keyset pagination requires at least one column');

        $parsed = sql_parse('SELECT * FROM users ORDER BY id');

        $modifier = new KeysetPaginationModifier(new KeysetPaginationConfig(10, []));
        $parsed->traverse($modifier);
    }
}

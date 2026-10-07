<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Unit\AST\Transformers;

use Flow\PostgreSql\AST\Transformers\KeysetColumn;
use Flow\PostgreSql\AST\Transformers\KeysetSortClause;
use Flow\PostgreSql\AST\Transformers\SortOrder;
use Flow\PostgreSql\Exception\PaginationException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function extension_loaded;
use function Flow\PostgreSql\DSL\sql_keyset_column;
use function Flow\PostgreSql\DSL\sql_parse;
use function sprintf;

final class KeysetSortClauseTest extends TestCase
{
    /**
     * @return Generator<string, array{string, list<KeysetColumn>}>
     */
    public static function matchingProvider(): Generator
    {
        yield 'default direction' => ['SELECT * FROM t ORDER BY id', [sql_keyset_column('id')]];
        yield 'explicit asc' => ['SELECT * FROM t ORDER BY id ASC', [sql_keyset_column('id')]];
        yield 'qualified order by, plain key' => ['SELECT * FROM t u ORDER BY u.id', [sql_keyset_column('id')]];
        yield 'plain order by, qualified key' => ['SELECT * FROM t u ORDER BY id', [sql_keyset_column('u.id')]];
        yield 'all desc' => [
            'SELECT * FROM t ORDER BY created_at DESC, id DESC',
            [sql_keyset_column('created_at', SortOrder::DESC), sql_keyset_column('id', SortOrder::DESC)],
        ];
        yield 'documented composite' => [
            'SELECT * FROM users ORDER BY created_at, id',
            [sql_keyset_column('created_at'), sql_keyset_column('id')],
        ];
    }

    /**
     * @return Generator<string, array{string, list<KeysetColumn>, string}>
     */
    public static function mismatchProvider(): Generator
    {
        $item = 'Keyset pagination requires ORDER BY item #%d to be the key "%s" with no NULLS clause, ordinal, expression or USING';

        yield 'wrong count' => [
            'SELECT * FROM t ORDER BY id',
            [sql_keyset_column('created_at'), sql_keyset_column('id')],
            'Keyset pagination requires ORDER BY to list exactly the keys "created_at ASC, id ASC", but the query orders by 1 item(s)',
        ];
        yield 'wrong column' => [
            'SELECT * FROM t ORDER BY name',
            [sql_keyset_column('id')],
            sprintf($item, 1, 'id ASC'),
        ];
        yield 'wrong direction' => [
            'SELECT * FROM t ORDER BY created_at, id DESC',
            [sql_keyset_column('created_at'), sql_keyset_column('id')],
            sprintf($item, 2, 'id ASC'),
        ];
        yield 'nulls clause' => [
            'SELECT * FROM t ORDER BY id NULLS FIRST',
            [sql_keyset_column('id')],
            sprintf($item, 1, 'id ASC'),
        ];
        yield 'ordinal' => ['SELECT * FROM t ORDER BY 1', [sql_keyset_column('id')], sprintf($item, 1, 'id ASC')];
        yield 'expression' => [
            'SELECT * FROM t ORDER BY lower(name)',
            [sql_keyset_column('name')],
            sprintf($item, 1, 'name ASC'),
        ];
        yield 'using operator' => [
            'SELECT * FROM t ORDER BY id USING <',
            [sql_keyset_column('id')],
            sprintf($item, 1, 'id ASC'),
        ];
    }

    protected function setUp(): void
    {
        if (!extension_loaded('pg_query')) {
            self::markTestSkipped(
                'pg_query extension is not loaded. For local development use `nix-shell --arg with-pg-query-ext true` to enable it in the shell.',
            );
        }
    }

    public function test_describe_lists_keys_with_directions(): void
    {
        static::assertSame(
            '"created_at DESC, id ASC"',
            (new KeysetSortClause([
                sql_keyset_column('created_at', SortOrder::DESC),
                sql_keyset_column('id'),
            ]))->describe(),
        );
    }

    /**
     * @param list<KeysetColumn> $columns
     */
    #[DataProvider('matchingProvider')]
    public function test_order_by_equal_to_the_keys_is_accepted(string $sql, array $columns): void
    {
        $this->expectNotToPerformAssertions();

        (new KeysetSortClause($columns))->assertMatches(
            sql_parse($sql)->statements()->assertReadOnlySelect()->raw()->getSortClause(),
        );
    }

    /**
     * @param list<KeysetColumn> $columns
     */
    #[DataProvider('mismatchProvider')]
    public function test_order_by_different_from_the_keys_is_rejected(
        string $sql,
        array $columns,
        string $message,
    ): void {
        $this->expectException(PaginationException::class);
        $this->expectExceptionMessage($message);

        (new KeysetSortClause($columns))->assertMatches(
            sql_parse($sql)->statements()->assertReadOnlySelect()->raw()->getSortClause(),
        );
    }
}

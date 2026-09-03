<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use Flow\PostgreSql\Tests\Integration\PostgreSqlTestCase;

use function Flow\PostgreSql\DSL\cast;
use function Flow\PostgreSql\DSL\column_type_bigint;
use function Flow\PostgreSql\DSL\column_type_boolean;
use function Flow\PostgreSql\DSL\column_type_integer;
use function Flow\PostgreSql\DSL\column_type_text;
use function Flow\PostgreSql\DSL\constructor_mapper;
use function Flow\PostgreSql\DSL\func;
use function Flow\PostgreSql\DSL\is_true;
use function Flow\PostgreSql\DSL\literal;
use function Flow\PostgreSql\DSL\row_expr;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\values_table;
use function iterator_to_array;

final class PgSqlCursorTest extends PostgreSqlTestCase
{
    public function test_cursor_count(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(func('generate_series', [literal(1), literal(5)])->as('num')));

        static::assertSame(5, $cursor->count());
    }

    public function test_cursor_free(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(literal(1)->as('num')));

        $cursor->free();

        static::assertSame(0, $cursor->count());
        static::assertNull($cursor->next());
    }

    public function test_cursor_iterate_method(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(func('generate_series', [literal(1), literal(2)])->as('num')));

        $rows = iterator_to_array($cursor->iterate());

        static::assertCount(2, $rows);
    }

    public function test_cursor_iterates_rows(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(func('generate_series', [literal(1), literal(3)])->as('num')));

        $rows = [];

        foreach ($cursor as $row) {
            $rows[] = $row;
        }

        static::assertCount(3, $rows);
        static::assertNotNull($rows[0]);
        static::assertNotNull($rows[1]);
        static::assertNotNull($rows[2]);
        static::assertSame(1, $rows[0]['num']);
        static::assertSame(2, $rows[1]['num']);
        static::assertSame(3, $rows[2]['num']);
    }

    public function test_cursor_map_to_objects(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(
                select(star())
                    ->from(values_table(
                        row_expr([literal(1), literal('Alice')]),
                        row_expr([literal(2), literal('Bob')]),
                    )->as('t', ['id', 'name'])),
            );

        $objects = iterator_to_array($cursor->map(constructor_mapper(CursorTestUser::class)));

        static::assertCount(2, $objects);
        static::assertInstanceOf(CursorTestUser::class, $objects[0]);
        static::assertSame('Alice', $objects[0]->name);
        static::assertInstanceOf(CursorTestUser::class, $objects[1]);
        static::assertSame('Bob', $objects[1]->name);
    }

    public function test_cursor_next_method(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(func('generate_series', [literal(1), literal(3)])->as('num')));

        $row1 = $cursor->next();
        static::assertNotNull($row1);
        static::assertSame(1, $row1['num']);

        $row2 = $cursor->next();
        static::assertNotNull($row2);
        static::assertSame(2, $row2['num']);

        $row3 = $cursor->next();
        static::assertNotNull($row3);
        static::assertSame(3, $row3['num']);

        static::assertNull($cursor->next());
    }

    public function test_cursor_with_type_conversion(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(
                cast(literal(42), column_type_integer())->as('num'),
                cast(literal(true), column_type_boolean())->as('flag'),
            ));

        $row = $cursor->next();

        static::assertNotNull($row);
        static::assertSame(42, $row['num']);
        static::assertTrue($row['flag']);
    }

    public function test_duplicate_output_names_are_cast_by_name_not_position(): void
    {
        // pg_fetch_assoc() collapses duplicate output names last-wins, so a positional type lookup
        // applies the wrong column's type to the surviving value and corrupts it in both directions.
        static::assertSame(
            [['a' => 'x']],
            iterator_to_array(
                $this
                    ->pgsqlContext()
                    ->client()
                    ->cursor(select(
                        cast(literal(1), column_type_bigint())->as('a'),
                        cast(literal('x'), column_type_text())->as('a'),
                    ))
                    ->iterate(),
            ),
        );

        static::assertSame(
            [['a' => 1]],
            iterator_to_array(
                $this
                    ->pgsqlContext()
                    ->client()
                    ->cursor(select(
                        cast(literal('x'), column_type_text())->as('a'),
                        cast(literal(1), column_type_bigint())->as('a'),
                    ))
                    ->iterate(),
            ),
        );
    }

    public function test_empty_cursor(): void
    {
        $cursor = $this
            ->pgsqlContext()
            ->client()
            ->cursor(select(literal(1))->where(is_true(literal(false))));

        static::assertSame(0, $cursor->count());
        static::assertNull($cursor->next());
        static::assertSame([], iterator_to_array($cursor));
    }
}

final readonly class CursorTestUser
{
    public function __construct(
        public int $id,
        public string $name,
    ) {}
}

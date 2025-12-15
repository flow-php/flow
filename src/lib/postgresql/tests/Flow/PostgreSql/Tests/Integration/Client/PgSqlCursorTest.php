<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Tests\Integration\Client;

use function Flow\PostgreSql\DSL\{func, literal, select};

final class PgSqlCursorTest extends ClientTestCase
{
    public function test_cursor_count() : void
    {
        $cursor = $this->client->cursor(
            select(func('generate_series', [literal(1), literal(5)])->as('num'))
        );

        self::assertSame(5, $cursor->count());
    }

    public function test_cursor_free() : void
    {
        $cursor = $this->client->cursor(
            select(literal(1)->as('num'))
        );

        $cursor->free();

        self::assertSame(0, $cursor->count());
        self::assertNull($cursor->next());
    }

    public function test_cursor_iterate_method() : void
    {
        $cursor = $this->client->cursor(
            select(func('generate_series', [literal(1), literal(2)])->as('num'))
        );

        $rows = \iterator_to_array($cursor->iterate());

        self::assertCount(2, $rows);
    }

    public function test_cursor_iterates_rows() : void
    {
        $cursor = $this->client->cursor(
            select(func('generate_series', [literal(1), literal(3)])->as('num'))
        );

        $rows = [];

        foreach ($cursor as $row) {
            $rows[] = $row;
        }

        self::assertCount(3, $rows);
        self::assertSame(1, $rows[0]['num']);
        self::assertSame(2, $rows[1]['num']);
        self::assertSame(3, $rows[2]['num']);
    }

    public function test_cursor_map_to_objects() : void
    {
        $cursor = $this->client->cursor(
            "SELECT * FROM (VALUES (1, 'Alice'), (2, 'Bob')) AS t(id, name)"
        );

        $objects = \iterator_to_array($cursor->map(CursorTestUser::class));

        self::assertCount(2, $objects);
        self::assertInstanceOf(CursorTestUser::class, $objects[0]);
        self::assertSame('Alice', $objects[0]->name);
        self::assertInstanceOf(CursorTestUser::class, $objects[1]);
        self::assertSame('Bob', $objects[1]->name);
    }

    public function test_cursor_next_method() : void
    {
        $cursor = $this->client->cursor(
            select(func('generate_series', [literal(1), literal(3)])->as('num'))
        );

        $row1 = $cursor->next();
        self::assertNotNull($row1);
        self::assertSame(1, $row1['num']);

        $row2 = $cursor->next();
        self::assertNotNull($row2);
        self::assertSame(2, $row2['num']);

        $row3 = $cursor->next();
        self::assertNotNull($row3);
        self::assertSame(3, $row3['num']);

        self::assertNull($cursor->next());
    }

    public function test_cursor_with_type_conversion() : void
    {
        $cursor = $this->client->cursor(
            'SELECT 42::integer AS num, true::boolean AS flag'
        );

        $row = $cursor->next();

        self::assertNotNull($row);
        self::assertSame(42, $row['num']);
        self::assertTrue($row['flag']);
    }

    public function test_empty_cursor() : void
    {
        $cursor = $this->client->cursor('SELECT 1 WHERE false');

        self::assertSame(0, $cursor->count());
        self::assertNull($cursor->next());
        self::assertSame([], \iterator_to_array($cursor));
    }
}

final readonly class CursorTestUser
{
    public function __construct(
        public int $id,
        public string $name,
    ) {
    }
}

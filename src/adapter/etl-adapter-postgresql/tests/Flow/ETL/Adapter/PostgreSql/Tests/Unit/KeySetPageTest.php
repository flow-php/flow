<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Unit;

use Flow\ETL\Adapter\PostgreSql\Pagination\KeySetPage;
use Flow\ETL\Adapter\PostgreSql\Pagination\KeyValues;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_desc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;

final class KeySetPageTest extends FlowTestCase
{
    public function test_a_composite_tie_with_mixed_directions_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires unique keys, but two rows share the key (a = 2, b = 1)',
        );

        KeySetPage::of(
            pgsql_pagination_key_set(pgsql_pagination_key_desc('a'), pgsql_pagination_key_asc('b')),
            [['a' => 2, 'b' => 1], ['a' => 2, 'b' => 1], ['a' => 1, 'b' => 0]],
            2,
            null,
        );
    }

    public function test_a_full_page_keeps_size_rows_and_points_at_the_lookahead(): void
    {
        $keySet = pgsql_pagination_key_set(pgsql_pagination_key_asc('id'));
        $page = KeySetPage::of($keySet, [['id' => 1], ['id' => 2], ['id' => 3]], 2, null);

        static::assertSame([['id' => 1], ['id' => 2]], $page->rows);
        static::assertFalse($page->isLast());
        static::assertEquals(KeyValues::of($keySet, ['id' => 2]), $page->cursor);
        static::assertEquals(KeyValues::of($keySet, ['id' => 3]), $page->lookahead);
    }

    public function test_a_page_not_starting_at_the_lookahead_is_rejected(): void
    {
        $keySet = pgsql_pagination_key_set(pgsql_pagination_key_asc('k'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            "Keyset pagination expected the next page to start at the key (k = 'A'), but it did not.",
        );

        KeySetPage::of($keySet, [['k' => 'b']], 1, KeyValues::of($keySet, ['k' => 'A']));
    }

    public function test_a_short_page_is_the_last(): void
    {
        $page = KeySetPage::of(
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            [['id' => 1], ['id' => 2]],
            2,
            null,
        );

        static::assertSame([['id' => 1], ['id' => 2]], $page->rows);
        static::assertTrue($page->isLast());
        static::assertNull($page->lookahead);
    }

    public function test_a_tie_inside_the_page_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires unique keys, but two rows share the key (id = 1); add a unique column (for example the primary key) to the key set as its least significant key',
        );

        KeySetPage::of(
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            [['id' => 1], ['id' => 1], ['id' => 2]],
            2,
            null,
        );
    }

    public function test_a_tie_with_the_lookahead_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key (id = 2)');

        KeySetPage::of(
            pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
            [['id' => 1], ['id' => 2], ['id' => 2]],
            2,
            null,
        );
    }

    public function test_an_empty_page_after_a_lookahead_is_rejected(): void
    {
        $keySet = pgsql_pagination_key_set(pgsql_pagination_key_asc('id'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination expected the next page to start at the key (id = 3), but it did not.',
        );

        KeySetPage::of($keySet, [], 2, KeyValues::of($keySet, ['id' => 3]));
    }
}

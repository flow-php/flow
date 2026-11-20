<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Doctrine\Tests\Unit\IO\Extractor\Pagination;

use Flow\ETL\Adapter\Doctrine\IO\Extractor\Pagination\KeySetPage;
use Flow\ETL\Adapter\Doctrine\IO\Extractor\Pagination\KeyValues;
use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\Adapter\Doctrine\pagination_key_asc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_desc;
use function Flow\ETL\Adapter\Doctrine\pagination_key_set;

final class KeySetPageTest extends FlowTestCase
{
    public function test_a_composite_tie_with_mixed_directions_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination requires unique keys, but two rows share the key (a = 2, b = 1)',
        );

        KeySetPage::of(
            pagination_key_set(pagination_key_asc('b'), pagination_key_desc('a')),
            ['key_a', 'key_b'],
            [['key_a' => 2, 'key_b' => 1], ['key_a' => 2, 'key_b' => 1], ['key_a' => 1, 'key_b' => 0]],
            2,
            null,
        );
    }

    public function test_a_full_page_keeps_size_rows_and_points_at_the_lookahead(): void
    {
        $keySet = pagination_key_set(pagination_key_asc('id'));
        $page = KeySetPage::of($keySet, ['key_id'], [['key_id' => 1], ['key_id' => 2], ['key_id' => 3]], 2, null);

        static::assertSame([['key_id' => 1], ['key_id' => 2]], $page->rows);
        static::assertFalse($page->isLast());
        static::assertEquals(KeyValues::of($keySet, ['key_id'], ['key_id' => 2]), $page->cursor);
        static::assertEquals(KeyValues::of($keySet, ['key_id'], ['key_id' => 3]), $page->lookahead);
    }

    public function test_a_page_not_starting_at_the_lookahead_is_rejected(): void
    {
        $keySet = pagination_key_set(pagination_key_asc('k'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            "Keyset pagination expected the next page to start at the key (k = 'A'), but it did not.",
        );

        KeySetPage::of($keySet, ['key_k'], [['key_k' => 'b']], 1, KeyValues::of($keySet, ['key_k'], ['key_k' => 'A']));
    }

    public function test_a_short_page_is_the_last(): void
    {
        $page = KeySetPage::of(
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['key_id' => 2]],
            2,
            null,
        );

        static::assertSame([['key_id' => 1], ['key_id' => 2]], $page->rows);
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
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['key_id' => 1], ['key_id' => 2]],
            2,
            null,
        );
    }

    public function test_a_tie_with_the_lookahead_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Keyset pagination requires unique keys, but two rows share the key (id = 2)');

        KeySetPage::of(
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['key_id' => 2], ['key_id' => 2]],
            2,
            null,
        );
    }

    public function test_an_empty_page_after_a_lookahead_is_rejected(): void
    {
        $keySet = pagination_key_set(pagination_key_asc('id'));

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'Keyset pagination expected the next page to start at the key (id = 3), but it did not.',
        );

        KeySetPage::of($keySet, ['key_id'], [], 2, KeyValues::of($keySet, ['key_id'], ['key_id' => 3]));
    }

    public function test_a_composite_key_differing_only_in_the_last_key_is_accepted(): void
    {
        $keySet = pagination_key_set(pagination_key_asc('a'), pagination_key_asc('b'));
        $page = KeySetPage::of(
            $keySet,
            ['key_a', 'key_b'],
            [['key_a' => 1, 'key_b' => 1], ['key_a' => 1, 'key_b' => 2], ['key_a' => 1, 'key_b' => 3]],
            2,
            null,
        );

        static::assertSame([['key_a' => 1, 'key_b' => 1], ['key_a' => 1, 'key_b' => 2]], $page->rows);
        static::assertEquals(KeyValues::of($keySet, ['key_a', 'key_b'], ['key_a' => 1, 'key_b' => 2]), $page->cursor);
        static::assertEquals(
            KeyValues::of($keySet, ['key_a', 'key_b'], ['key_a' => 1, 'key_b' => 3]),
            $page->lookahead,
        );
    }

    public function test_a_missing_key_column_in_a_middle_row_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Column "id" not found in result row for keyset pagination');

        KeySetPage::of(
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['other' => 2], ['key_id' => 3]],
            2,
            null,
        );
    }

    public function test_a_null_key_in_a_middle_row_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'NULL value found in column "id" for keyset pagination; key columns must be non-null',
        );

        KeySetPage::of(
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['key_id' => null], ['key_id' => 3]],
            2,
            null,
        );
    }

    public function test_an_invalid_key_after_a_tie_is_rejected_before_the_tie(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage(
            'NULL value found in column "id" for keyset pagination; key columns must be non-null',
        );

        KeySetPage::of(
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['key_id' => 1], ['key_id' => null]],
            2,
            null,
        );
    }

    public function test_an_unsupported_key_type_in_a_middle_row_is_rejected(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unsupported value type "array" in column "id" for keyset pagination');

        KeySetPage::of(
            pagination_key_set(pagination_key_asc('id')),
            ['key_id'],
            [['key_id' => 1], ['key_id' => [2]], ['key_id' => 3]],
            2,
            null,
        );
    }
}

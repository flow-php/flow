<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\str_entry;

final class CacheIndexTest extends FlowTestCase
{
    public function test_empty_index_round_trip(): void
    {
        $index = new CacheIndex('dataset-id');

        $indexRows = $index->toRows();

        static::assertCount(0, $indexRows);
        static::assertEquals($index, CacheIndex::fromRows('dataset-id', $indexRows));
    }

    public function test_from_rows_key_comes_from_the_argument(): void
    {
        $index = new CacheIndex('original-key');
        $index->add('chunk-1');

        $reconstructed = CacheIndex::fromRows('different-key', $index->toRows());

        static::assertSame('different-key', $reconstructed->key);
        static::assertSame(['chunk-1'], $reconstructed->values());
    }

    public function test_from_rows_with_missing_key_entry_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);

        CacheIndex::fromRows('dataset-id', rows(row(int_entry('id', 1))));
    }

    public function test_from_rows_with_non_string_key_entry_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CacheIndex expects rows with a string "key" entry, got: null');

        CacheIndex::fromRows('dataset-id', rows(row(str_entry('key', null))));
    }

    public function test_to_rows_from_rows_round_trip_preserves_order(): void
    {
        $index = new CacheIndex('dataset-id');
        $index->add('chunk-b');
        $index->add('chunk-a');
        $index->add('chunk-c');

        $reconstructed = CacheIndex::fromRows('dataset-id', $index->toRows());

        static::assertEquals($index, $reconstructed);
        static::assertSame(['chunk-b', 'chunk-a', 'chunk-c'], $reconstructed->values());
    }

    public function test_to_rows_stores_one_key_entry_per_row(): void
    {
        $index = new CacheIndex('dataset-id');
        $index->add('chunk-1');
        $index->add('chunk-2');

        static::assertEquals(
            rows(row(str_entry('key', 'chunk-1')), row(str_entry('key', 'chunk-2'))),
            $index->toRows(),
        );
    }
}

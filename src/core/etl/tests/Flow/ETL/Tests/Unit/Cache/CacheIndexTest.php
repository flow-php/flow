<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Cache;

use Flow\ETL\Cache\CacheIndex;
use Flow\ETL\Cardinality;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Tests\FlowTestCase;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\row;
use function Flow\ETL\DSL\rows;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;

final class CacheIndexTest extends FlowTestCase
{
    public function test_empty_index_round_trip(): void
    {
        $index = new CacheIndex('dataset-id');

        $indexRows = $index->toRows();

        static::assertCount(0, $indexRows);
        static::assertEquals($index, CacheIndex::fromRows('dataset-id', $indexRows));
    }

    public function test_an_empty_index_declares_zero_rows_exactly(): void
    {
        static::assertEquals(Cardinality::exact(0), (new CacheIndex('dataset-id'))->rows());
    }

    public function test_an_index_without_a_rows_column_declares_unknown_rows(): void
    {
        $index = CacheIndex::fromRows('dataset-id', rows(schema(str_schema('key')), row(['key' => 'chunk-1'])));

        static::assertSame(['chunk-1'], $index->values());
        static::assertEquals(Cardinality::unknown(), $index->rows());
    }

    public function test_a_chunk_without_a_count_makes_the_rows_unknown(): void
    {
        $index = new CacheIndex('dataset-id');
        $index->add('chunk-1', 3);
        $index->add('chunk-2');

        static::assertEquals(Cardinality::unknown(), $index->rows());
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

        CacheIndex::fromRows('dataset-id', rows(schema(int_schema('id')), row(['id' => 1])));
    }

    public function test_from_rows_with_non_string_key_entry_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CacheIndex expects rows with a string "key" entry, got: null');

        CacheIndex::fromRows('dataset-id', rows(schema(str_schema('key', nullable: true)), row(['key' => null])));
    }

    public function test_from_rows_with_non_integer_rows_entry_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('CacheIndex expects rows with an integer "rows" entry, got: string');

        CacheIndex::fromRows('dataset-id', rows(
            schema(str_schema('key'), str_schema('rows')),
            row(['key' => 'chunk-1', 'rows' => 'three']),
        ));
    }

    public function test_rows_sum_the_chunk_counts(): void
    {
        $index = new CacheIndex('dataset-id');
        $index->add('chunk-1', 3);
        $index->add('chunk-2', 0);
        $index->add('chunk-3', 4);

        static::assertEquals(Cardinality::exact(7), CacheIndex::fromRows('dataset-id', $index->toRows())->rows());
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

    public function test_to_rows_stores_one_key_and_rows_entry_per_chunk(): void
    {
        $index = new CacheIndex('dataset-id');
        $index->add('chunk-1', 2);
        $index->add('chunk-2');

        static::assertEquals(
            rows(
                schema(str_schema('key'), int_schema('rows', nullable: true)),
                row(['key' => 'chunk-1', 'rows' => 2]),
                row(['key' => 'chunk-2', 'rows' => null]),
            ),
            $index->toRows(),
        );
    }
}

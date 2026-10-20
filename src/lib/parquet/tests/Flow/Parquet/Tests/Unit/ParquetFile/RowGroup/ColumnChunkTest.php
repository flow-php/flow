<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\RowGroup;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Index\IndexLocation;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use Flow\Parquet\ThriftModel\ColumnChunk as ThriftColumnChunk;
use Flow\Parquet\ThriftModel\ColumnMetaData;
use PHPUnit\Framework\TestCase;

final class ColumnChunkTest extends TestCase
{
    public function test_a_column_chunk_without_metadata_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Column chunk at offset 4 has no metadata (an encrypted column is not supported)',
        );

        ColumnChunk::fromThrift(new ThriftColumnChunk(['file_offset' => 4]));
    }

    public function test_page_index_locations_round_trip_through_thrift(): void
    {
        $chunk = new ColumnChunk(
            PhysicalType::INT64,
            Compressions::UNCOMPRESSED,
            2,
            4,
            ['id'],
            [],
            10,
            10,
            null,
            4,
            null,
            null,
            new IndexLocation(100, 20),
            new IndexLocation(120, 15),
        );

        static::assertEquals($chunk, ColumnChunk::fromThrift($chunk->toThrift()));
        static::assertEquals(new IndexLocation(100, 20), $chunk->columnIndex());
        static::assertEquals(new IndexLocation(120, 15), $chunk->offsetIndex());
    }

    public function test_a_bloom_filter_location_round_trips_through_thrift_and_survives_page_index_locations(): void
    {
        $chunk = ColumnChunkMother::withBloomFilter(FlatColumn::int64('id'), 200, 48);

        static::assertEquals($chunk, ColumnChunk::fromThrift($chunk->toThrift()));
        static::assertSame(200, $chunk->bloomFilterOffset());
        static::assertSame(48, $chunk->bloomFilterLength());

        $indexed = $chunk->withLocations(offsetIndex: new IndexLocation(120, 15));

        static::assertSame(200, $indexed->bloomFilterOffset());
        static::assertSame(48, $indexed->bloomFilterLength());
        static::assertEquals(new IndexLocation(120, 15), $indexed->offsetIndex());
    }

    public function test_a_chunk_records_the_bloom_filter_written_for_it(): void
    {
        $chunk = ColumnChunkMother::withIndexes(
            FlatColumn::int64('id'),
            null,
            new IndexLocation(120, 15),
        )->withLocations(bloomFilterOffset: 300, bloomFilterLength: 64);

        static::assertSame(300, $chunk->bloomFilterOffset());
        static::assertSame(64, $chunk->bloomFilterLength());
        static::assertEquals(new IndexLocation(120, 15), $chunk->offsetIndex());
    }

    public function test_a_location_no_index_can_be_read_from_is_no_location(): void
    {
        $thrift = (new ColumnChunk(
            PhysicalType::INT64,
            Compressions::UNCOMPRESSED,
            2,
            4,
            ['id'],
            [],
            10,
            10,
            null,
            4,
            null,
            null,
        ))->toThrift();
        $thrift->column_index_offset = 100;
        $thrift->column_index_length = 0;
        $thrift->offset_index_offset = -1;
        $thrift->offset_index_length = 15;

        static::assertNull(ColumnChunk::fromThrift($thrift)->columnIndex());
        static::assertNull(ColumnChunk::fromThrift($thrift)->offsetIndex());
    }

    public function test_a_bloom_filter_length_without_its_offset_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('A column chunk records a bloom filter length without its offset');

        new ColumnChunk(
            PhysicalType::INT64,
            Compressions::UNCOMPRESSED,
            2,
            4,
            ['id'],
            [],
            10,
            10,
            null,
            4,
            null,
            null,
            bloomFilterLength: 48,
        );
    }

    public function test_a_bloom_filter_length_read_without_its_offset_is_dropped(): void
    {
        $chunk = ColumnChunk::fromThrift(new ThriftColumnChunk([
            'file_offset' => 4,
            'meta_data' => new ColumnMetaData([
                'type' => 2,
                'encodings' => [],
                'path_in_schema' => ['id'],
                'codec' => 0,
                'num_values' => 2,
                'total_uncompressed_size' => 0,
                'total_compressed_size' => 0,
                'data_page_offset' => 4,
                'bloom_filter_length' => 48,
            ]),
        ]));

        static::assertNull($chunk->bloomFilterOffset());
        static::assertNull($chunk->bloomFilterLength());
    }
}

<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Index;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\ParquetFile\Index\IndexLocation;
use Flow\Parquet\ParquetFile\Index\StreamIndexes;
use Flow\Parquet\ParquetFile\PageIndex\BoundaryOrder;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\PageIndex\PageLocation;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\ThriftBytes;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Double\SpyFooter;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use Flow\Parquet\Tests\Mother\SbbfMother;
use Flow\Parquet\ThriftModel\BloomFilterAlgorithm;
use Flow\Parquet\ThriftModel\BloomFilterCompression;
use Flow\Parquet\ThriftModel\BloomFilterHash;
use Flow\Parquet\ThriftModel\BloomFilterHeader;
use Flow\Parquet\ThriftModel\ColumnIndex as ThriftColumnIndex;
use Flow\Parquet\ThriftModel\OffsetIndex as ThriftOffsetIndex;
use Flow\Parquet\ThriftModel\SplitBlockAlgorithm;
use Flow\Parquet\ThriftModel\Uncompressed;
use Flow\Parquet\ThriftModel\XxHash;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function pack;
use function strlen;

final class StreamIndexesTest extends TestCase
{
    public function test_an_index_is_read_from_its_offset_once_and_only_for_the_column_asked(): void
    {
        $id = FlatColumn::int64('id');
        $name = FlatColumn::string('name');
        $columnIndex = new ColumnIndex([false], [pack('P', 1)], [pack('P', 9)], BoundaryOrder::ASCENDING, [0]);
        $offsetIndex = new OffsetIndex([new PageLocation(4, 20, 0)]);
        $columnBytes = ThriftBytes::of($columnIndex->toThrift());
        $offsetBytes = ThriftBytes::of($offsetIndex->toThrift());
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1' . $columnBytes . $offsetBytes));
        $footer = new SpyFooter(Schema::with($id, $name), 2, [new RowGroup([
            ColumnChunkMother::withIndexes(
                $id,
                new IndexLocation(4, strlen($columnBytes)),
                new IndexLocation(4 + strlen($columnBytes), strlen($offsetBytes)),
            ),
            ColumnChunkMother::withStatistics($name, null),
        ], 2)]);
        $indexes = new StreamIndexes($footer, $stream);

        static::assertEquals($columnIndex, $indexes->columnIndex(0, $id));
        static::assertEquals($columnIndex, $indexes->columnIndex(0, $id));
        static::assertEquals($offsetIndex, $indexes->offsetIndex(0, $id));
        static::assertEquals($offsetIndex, $indexes->offsetIndex(0, $id));
        static::assertSame(2, $stream->reads);
        static::assertSame(strlen($columnBytes) + strlen($offsetBytes), $stream->bytes);
        static::assertEquals([[$id]], $footer->rowGroupChunkReads);
    }

    public function test_a_bloom_filter_is_read_once(): void
    {
        $id = FlatColumn::int64('id');
        $sbbf = SbbfMother::with($id, 3);
        $bytes = $sbbf->serialized();
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1' . $bytes));
        $indexes = new StreamIndexes(new SpyFooter(Schema::with($id), 2, [new RowGroup(
            [ColumnChunkMother::withBloomFilter($id, 4, strlen($bytes))],
            2,
        )]), $stream);

        static::assertEquals($sbbf, $indexes->bloomFilter(0, $id));
        static::assertEquals($sbbf, $indexes->bloomFilter(0, $id));
        static::assertSame(1, $stream->reads);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function malformed_bloom_filters(): iterable
    {
        yield 'not thrift' => ["\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"];
        yield 'no bytes declared' => [ThriftBytes::of(new BloomFilterHeader([
            'numBytes' => 0,
            'algorithm' => new BloomFilterAlgorithm(['BLOCK' => new SplitBlockAlgorithm()]),
            'hash' => new BloomFilterHash(['XXHASH' => new XxHash()]),
            'compression' => new BloomFilterCompression(['UNCOMPRESSED' => new Uncompressed()]),
        ]))];
        yield 'fewer bytes than declared' => [
            ThriftBytes::of(new BloomFilterHeader([
                'numBytes' => 32,
                'algorithm' => new BloomFilterAlgorithm(['BLOCK' => new SplitBlockAlgorithm()]),
                'hash' => new BloomFilterHash(['XXHASH' => new XxHash()]),
                'compression' => new BloomFilterCompression(['UNCOMPRESSED' => new Uncompressed()]),
            ])) . 'short',
        ];
    }

    #[DataProvider('malformed_bloom_filters')]
    public function test_a_bloom_filter_that_fails_to_decode_is_none(string $bytes): void
    {
        $id = FlatColumn::int64('id');

        static::assertNull((new StreamIndexes(new SpyFooter(Schema::with($id), 2, [new RowGroup(
            [ColumnChunkMother::withBloomFilter($id, 4, strlen($bytes))],
            2,
        )]), new MemorySourceStream('PAR1' . $bytes)))->bloomFilter(0, $id));
    }

    public function test_a_chunk_without_indexes_reads_nothing(): void
    {
        $name = FlatColumn::string('name');
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1'));
        $indexes = new StreamIndexes(new SpyFooter(Schema::with($name), 2, [new RowGroup(
            [ColumnChunkMother::withStatistics($name, null)],
            2,
        )]), $stream);

        static::assertNull($indexes->columnIndex(0, $name));
        static::assertNull($indexes->offsetIndex(0, $name));
        static::assertNull($indexes->bloomFilter(0, $name));
        static::assertSame(0, $stream->reads);
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function malformed_column_indexes(): iterable
    {
        yield 'not thrift' => ["\xFF\xFF\xFF\xFF"];
        yield 'unknown boundary order' => [ThriftBytes::of(new ThriftColumnIndex([
            'null_pages' => [false],
            'min_values' => ['a'],
            'max_values' => ['b'],
            'boundary_order' => 7,
        ]))];
        yield 'no null pages' => [ThriftBytes::of(new ThriftColumnIndex([
            'min_values' => ['a'],
            'max_values' => ['b'],
            'boundary_order' => 0,
        ]))];
        yield 'lists of other lengths' => [ThriftBytes::of(new ThriftColumnIndex([
            'null_pages' => [false, false],
            'min_values' => ['a'],
            'max_values' => ['b'],
            'boundary_order' => 0,
        ]))];
    }

    #[DataProvider('malformed_column_indexes')]
    public function test_a_column_index_that_fails_to_decode_is_none(string $bytes): void
    {
        $id = FlatColumn::int64('id');

        static::assertNull((new StreamIndexes(new SpyFooter(Schema::with($id), 2, [new RowGroup([
            ColumnChunkMother::withIndexes($id, new IndexLocation(4, strlen($bytes)), null),
        ], 2)]), new MemorySourceStream('PAR1' . $bytes)))->columnIndex(0, $id));
    }

    /**
     * @return iterable<string, array{string}>
     */
    public static function malformed_offset_indexes(): iterable
    {
        yield 'not thrift' => ["\xFF\xFF\xFF\xFF"];
        yield 'no pages' => [ThriftBytes::of(new ThriftOffsetIndex(['page_locations' => []]))];
        yield 'no page locations' => [ThriftBytes::of(new ThriftOffsetIndex([]))];
        yield 'a page outside the chunk' => [ThriftBytes::of(
            (new OffsetIndex([new PageLocation(20, 10, 0)]))->toThrift(),
        )];
        yield 'an empty page' => [ThriftBytes::of((new OffsetIndex([new PageLocation(4, 0, 0)]))->toThrift())];
        yield 'pages out of order' => [ThriftBytes::of(
            (new OffsetIndex([
                new PageLocation(14, 10, 0),
                new PageLocation(4, 10, 1),
            ]))->toThrift(),
        )];
        yield 'rows past the row group' => [ThriftBytes::of(
            (new OffsetIndex([
                new PageLocation(4, 10, 0),
                new PageLocation(14, 10, 2),
            ]))->toThrift(),
        )];
    }

    #[DataProvider('malformed_offset_indexes')]
    public function test_an_offset_index_that_fails_to_decode_or_does_not_fit_its_chunk_is_none(string $bytes): void
    {
        $id = FlatColumn::int64('id');

        static::assertNull((new StreamIndexes(new SpyFooter(Schema::with($id), 2, [new RowGroup([
            ColumnChunkMother::withIndexes($id, null, new IndexLocation(4, strlen($bytes))),
        ], 2)]), new MemorySourceStream('PAR1' . $bytes)))->offsetIndex(0, $id));
    }
}

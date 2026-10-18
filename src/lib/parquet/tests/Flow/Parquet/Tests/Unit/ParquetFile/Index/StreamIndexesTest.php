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
}

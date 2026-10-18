<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BloomFilter;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\BloomFilter\BloomFilterReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Tests\Context\BloomFilterBytes;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use Flow\Parquet\Tests\Mother\ColumnChunkMother;
use Flow\Parquet\Tests\Mother\SbbfMother;
use Flow\Parquet\ThriftModel\BloomFilterAlgorithm;
use Flow\Parquet\ThriftModel\BloomFilterCompression;
use Flow\Parquet\ThriftModel\BloomFilterHash;
use PHPUnit\Framework\TestCase;

use function str_repeat;
use function strlen;

final class BloomFilterReaderTest extends TestCase
{
    public function test_a_filter_is_read_at_once_when_its_length_is_known(): void
    {
        $id = FlatColumn::int64('id');
        $sbbf = SbbfMother::with($id, 7);
        $bytes = $sbbf->serialized();
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1' . $bytes));

        static::assertEquals(
            $sbbf,
            (new BloomFilterReader($stream))->read(ColumnChunkMother::withBloomFilter($id, 4, strlen($bytes))),
        );
        static::assertSame(1, $stream->reads);
    }

    public function test_a_filter_of_unknown_length_reads_its_header_then_its_bitset(): void
    {
        $id = FlatColumn::int64('id');
        $sbbf = SbbfMother::with($id, 7);
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1' . $sbbf->serialized()));

        static::assertEquals(
            $sbbf,
            (new BloomFilterReader($stream))->read(ColumnChunkMother::withBloomFilter($id, 4, null)),
        );
        static::assertSame(2, $stream->reads);
    }

    public function test_a_length_no_filter_fits_in_is_an_unknown_length(): void
    {
        $id = FlatColumn::int64('id');
        $sbbf = SbbfMother::with($id, 7);
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1' . $sbbf->serialized()));

        static::assertEquals(
            $sbbf,
            (new BloomFilterReader($stream))->read(ColumnChunkMother::withBloomFilter($id, 4, 0)),
        );
        static::assertSame(2, $stream->reads);
    }

    public function test_a_header_declaring_no_bitset_is_refused(): void
    {
        $bytes = BloomFilterBytes::of('', 0);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The bloom filter at offset 4 declares 0 bytes');

        (new BloomFilterReader(new MemorySourceStream('PAR1' . $bytes)))->read(ColumnChunkMother::withBloomFilter(
            FlatColumn::int64('id'),
            4,
            strlen($bytes),
        ));
    }

    public function test_a_chunk_without_a_filter_reads_nothing(): void
    {
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1'));

        static::assertNull((new BloomFilterReader($stream))->read(ColumnChunkMother::withStatistics(
            FlatColumn::int64('id'),
            null,
        )));
        static::assertSame(0, $stream->reads);
    }

    public function test_a_filter_of_an_unknown_algorithm_hash_or_compression_is_no_filter(): void
    {
        $id = FlatColumn::int64('id');
        $bitset = str_repeat("\0", 32);

        foreach ([
            BloomFilterBytes::of($bitset, algorithm: new BloomFilterAlgorithm()),
            BloomFilterBytes::of($bitset, hash: new BloomFilterHash()),
            BloomFilterBytes::of($bitset, compression: new BloomFilterCompression()),
        ] as $bytes) {
            static::assertNull((new BloomFilterReader(new MemorySourceStream('PAR1'
            . $bytes)))->read(ColumnChunkMother::withBloomFilter($id, 4, strlen($bytes))));
        }
    }

    public function test_an_offset_no_filter_can_be_read_from_is_no_filter(): void
    {
        $stream = new ReadCountingSourceStream(new MemorySourceStream('PAR1'));

        static::assertNull((new BloomFilterReader($stream))->read(ColumnChunkMother::withBloomFilter(
            FlatColumn::int64('id'),
            -1,
            40,
        )));
        static::assertSame(0, $stream->reads);
    }

    public function test_a_bitset_shorter_than_its_header_declares_is_refused(): void
    {
        $bytes = BloomFilterBytes::of(str_repeat("\0", 32), 64);

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('The bloom filter at offset 4 declares 64 bytes, 32 were read');

        (new BloomFilterReader(new MemorySourceStream('PAR1' . $bytes)))->read(ColumnChunkMother::withBloomFilter(
            FlatColumn::int64('id'),
            4,
            strlen($bytes),
        ));
    }
}

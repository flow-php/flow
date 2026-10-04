<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Engine;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use Flow\Parquet\Tests\Double\ReadCountingSourceStream;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_column;
use function iterator_to_array;

final class PhpParquetFileReaderTest extends TestCase
{
    public function test_the_footer_is_decoded_once(): void
    {
        $stream = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $reader = new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options());

        $metadata = $reader->metadata();
        $reads = $stream->reads;

        static::assertSame($metadata, $reader->metadata());
        static::assertSame($metadata->schema(), $reader->schema());
        static::assertSame(6, $reader->rowsNumber());
        static::assertSame($reads, $stream->reads);
    }

    public function test_total_byte_size_sums_the_row_groups(): void
    {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );
        $expected = 0;

        foreach ($reader->metadata()->rowGroups()->all() as $rowGroup) {
            $expected += $rowGroup->totalByteSize();
        }

        static::assertCount(3, $reader->metadata()->rowGroups()->all());
        static::assertGreaterThan(0, $expected);
        static::assertSame($expected, $reader->totalByteSize());
    }

    /**
     * @param int<1, max> $batchSize
     * @param list<list<int>> $ids
     */
    #[TestWith([4, null, null, [[1, 2, 3, 4], [5, 6]]])]
    #[TestWith([2, 3, 2, [[3, 4], [5]]])]
    #[TestWith([2, null, 6, []])]
    #[TestWith([2, null, 7, []])]
    public function test_read_columns_chunks_the_rows(int $batchSize, ?int $limit, ?int $offset, array $ids): void
    {
        $reader = new PhpParquetFileReader(
            new MemorySourceStream(MemoryParquetFile::threeRowGroups()),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        );
        $chunks = iterator_to_array($reader->readColumns(['id'], $batchSize, $limit, $offset), false);

        static::assertSame($ids, array_column($chunks, 'id'));
    }

    public function test_close_closes_the_stream_and_every_later_call_throws(): void
    {
        $stream = new ReadCountingSourceStream(new MemorySourceStream(MemoryParquetFile::threeRowGroups()));
        $reader = new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options());
        $reader->metadata();

        $reader->close();

        static::assertTrue($stream->closed);

        foreach ([
            static fn() => $reader->metadata(),
            static fn() => $reader->schema(),
            static fn() => $reader->rowsNumber(),
            static fn() => $reader->totalByteSize(),
            static fn() => iterator_to_array($reader->readColumns(['id'], 1, null, null)),
            static fn() => $reader->close(),
        ] as $call) {
            try {
                $call();
                static::fail('A call on a closed reader must throw');
            } catch (RuntimeException $exception) {
                static::assertSame('Reader is not open', $exception->getMessage());
            }
        }
    }

    public function test_a_file_without_the_parquet_magic_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Given file is not valid Parquet file');

        (new PhpParquetFileReader(
            new MemorySourceStream('not a parquet file'),
            ByteOrder::LITTLE_ENDIAN,
            new Options(),
        ))->metadata();
    }
}

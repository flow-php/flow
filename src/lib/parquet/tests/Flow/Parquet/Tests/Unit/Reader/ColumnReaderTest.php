<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Reader;

use Flow\Filesystem\Stream\MemorySourceStream;
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\Dremel\DremelAssembler;
use Flow\Parquet\Engine\PhpParquetFileReader;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\DataConverter;
use Flow\Parquet\Reader\ColumnChunkReader;
use Flow\Parquet\Reader\ColumnReader;
use Flow\Parquet\Reader\PageReader;
use Flow\Parquet\Tests\Context\MemoryParquetFile;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function array_map;
use function iterator_to_array;

final class ColumnReaderTest extends TestCase
{
    /**
     * @param list<int> $ids
     */
    #[TestWith([null, null, [1, 2, 3, 4, 5, 6]])]
    #[TestWith([null, 3, [4, 5, 6]])]
    #[TestWith([2, 1, [2, 3]])]
    #[TestWith([3, 2, [3, 4, 5]])]
    #[TestWith([1, 5, [6]])]
    #[TestWith([null, 6, []])]
    public function test_a_flat_column_reads_across_row_groups(?int $limit, ?int $offset, array $ids): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->metadata();

        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            array_map(static fn(int $id): array => ['id' => $id], $ids),
            iterator_to_array(
                $reader->read($metadata->schema()->get('id'), $metadata, $stream, $limit, $offset),
                false,
            ),
        );
    }

    public function test_a_nested_column_reads_across_row_groups(): void
    {
        $stream = new MemorySourceStream(MemoryParquetFile::threeRowGroups());
        $metadata = (new PhpParquetFileReader($stream, ByteOrder::LITTLE_ENDIAN, new Options()))->metadata();

        $reader = new ColumnReader(
            new ColumnChunkReader(new PageReader(ByteOrder::LITTLE_ENDIAN, new Options()), new Options()),
            new DremelAssembler(DataConverter::initialize(new Options())),
        );

        static::assertSame(
            array_map(static fn(int $id): array => ['tags' => ['t' . $id]], [2, 3, 4]),
            iterator_to_array($reader->read($metadata->schema()->get('tags'), $metadata, $stream, 3, 1), false),
        );
    }
}

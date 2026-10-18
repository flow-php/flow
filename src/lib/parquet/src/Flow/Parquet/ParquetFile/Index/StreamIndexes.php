<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Index;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\BloomFilter\BloomFilterReader;
use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\ColumnIndex as ThriftColumnIndex;
use Flow\Parquet\ThriftModel\OffsetIndex as ThriftOffsetIndex;

use function array_key_exists;

/**
 * Reads an index only when it is asked for, once. The footer is asked for each column's chunks on its own: the arrow
 * engine's RowGroupChunks hold only the columns they were built for.
 */
final class StreamIndexes implements Indexes
{
    /**
     * @var array<string, array<int, null|Sbbf>>
     */
    private array $bloomFilters = [];

    /**
     * @var array<string, RowGroupChunks>
     */
    private array $chunks = [];

    /**
     * @var array<string, array<int, null|ColumnIndex>>
     */
    private array $columnIndexes = [];

    /**
     * @var array<string, array<int, null|OffsetIndex>>
     */
    private array $offsetIndexes = [];

    public function __construct(
        private readonly Footer $footer,
        private readonly SourceStream $stream,
    ) {}

    public function bloomFilter(int $rowGroup, FlatColumn $column): ?Sbbf
    {
        $path = $column->flatPath();

        if (!array_key_exists($rowGroup, $this->bloomFilters[$path] ?? [])) {
            $this->bloomFilters[$path][$rowGroup] = (new BloomFilterReader($this->stream))->read($this->chunks(
                $column,
            )->chunk($rowGroup, $column));
        }

        return $this->bloomFilters[$path][$rowGroup];
    }

    public function columnIndex(int $rowGroup, FlatColumn $column): ?ColumnIndex
    {
        $path = $column->flatPath();

        if (!array_key_exists($rowGroup, $this->columnIndexes[$path] ?? [])) {
            $chunk = $this->chunks($column)->chunk($rowGroup, $column);
            $thrift = new ThriftColumnIndex();
            $this->columnIndexes[$path][$rowGroup] = $this->read($thrift, $chunk->columnIndex())
                ? ColumnIndex::fromThrift($thrift)
                : null;
        }

        return $this->columnIndexes[$path][$rowGroup];
    }

    public function offsetIndex(int $rowGroup, FlatColumn $column): ?OffsetIndex
    {
        $path = $column->flatPath();

        if (!array_key_exists($rowGroup, $this->offsetIndexes[$path] ?? [])) {
            $chunk = $this->chunks($column)->chunk($rowGroup, $column);
            $thrift = new ThriftOffsetIndex();
            $this->offsetIndexes[$path][$rowGroup] = $this->read($thrift, $chunk->offsetIndex())
                ? OffsetIndex::fromThrift($thrift)
                : null;
        }

        return $this->offsetIndexes[$path][$rowGroup];
    }

    /**
     * Whether the chunk locates the index, which is then read into $thrift.
     */
    private function read(ThriftColumnIndex|ThriftOffsetIndex $thrift, ?IndexLocation $location): bool
    {
        if ($location === null) {
            return false;
        }

        $thrift->read(new CompactProtocol(new MemoryBuffer($this->stream->read($location->length, $location->offset))));

        return true;
    }

    private function chunks(FlatColumn $column): RowGroupChunks
    {
        return $this->chunks[$column->flatPath()] ??= $this->footer->rowGroupChunks([$column]);
    }
}

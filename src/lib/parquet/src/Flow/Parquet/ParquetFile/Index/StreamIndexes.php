<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Index;

use Flow\Filesystem\SourceStream;
use Flow\Parquet\BloomFilter\BloomFilterReader;
use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\PageIndex\ColumnIndex;
use Flow\Parquet\ParquetFile\PageIndex\OffsetIndex;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\ColumnIndex as ThriftColumnIndex;
use Flow\Parquet\ThriftModel\OffsetIndex as ThriftOffsetIndex;
use Thrift\Exception\TException;

use function array_key_exists;

/**
 * Footer is asked per column: arrow RowGroupChunks hold only the columns they were built for.
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

    private readonly BloomFilterReader $bloomFilterReader;

    public function __construct(
        private readonly Footer $footer,
        private readonly SourceStream $stream,
    ) {
        $this->bloomFilterReader = new BloomFilterReader($stream);
    }

    public function bloomFilter(int $rowGroup, FlatColumn $column): ?Sbbf
    {
        $path = $column->flatPath();

        if (!array_key_exists($rowGroup, $this->bloomFilters[$path] ?? [])) {
            try {
                $this->bloomFilters[$path][$rowGroup] = $this->bloomFilterReader->read($this->chunks($column)->chunk(
                    $rowGroup,
                    $column,
                ));
            } catch (InvalidArgumentException|RuntimeException|TException) {
                $this->bloomFilters[$path][$rowGroup] = null;
            }
        }

        return $this->bloomFilters[$path][$rowGroup];
    }

    public function columnIndex(int $rowGroup, FlatColumn $column): ?ColumnIndex
    {
        $path = $column->flatPath();

        if (!array_key_exists($rowGroup, $this->columnIndexes[$path] ?? [])) {
            $thrift = new ThriftColumnIndex();

            try {
                $this->columnIndexes[$path][$rowGroup] = $this->read(
                    $thrift,
                    $this->chunks($column)->chunk($rowGroup, $column)->columnIndex(),
                )
                    ? ColumnIndex::fromThrift($thrift)
                    : null;
            } catch (InvalidArgumentException|TException) {
                $this->columnIndexes[$path][$rowGroup] = null;
            }
        }

        return $this->columnIndexes[$path][$rowGroup];
    }

    public function offsetIndex(int $rowGroup, FlatColumn $column): ?OffsetIndex
    {
        $path = $column->flatPath();

        if (!array_key_exists($rowGroup, $this->offsetIndexes[$path] ?? [])) {
            $chunks = $this->chunks($column);
            $chunk = $chunks->chunk($rowGroup, $column);
            $thrift = new ThriftOffsetIndex();

            try {
                $offsetIndex = $this->read($thrift, $chunk->offsetIndex()) ? OffsetIndex::fromThrift($thrift) : null;
            } catch (TException) {
                $offsetIndex = null;
            }

            $this->offsetIndexes[$path][$rowGroup] = $offsetIndex !== null
            && $offsetIndex->fits($chunk, $chunks->rowCounts()[$rowGroup])
                ? $offsetIndex
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

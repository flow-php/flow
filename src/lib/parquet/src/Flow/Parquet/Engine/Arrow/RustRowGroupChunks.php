<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine\Arrow;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\OutOfBoundsException;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\Thrift\CompactProtocol;
use Flow\Parquet\Thrift\MemoryBuffer;
use Flow\Parquet\ThriftModel\ColumnChunk as ThriftColumnChunk;
use Flow\Parquet\ThriftModel\ColumnOrder as ThriftColumnOrder;

use function array_key_exists;

final class RustRowGroupChunks implements RowGroupChunks
{
    /**
     * @param list<int> $rowCounts
     * @param array<string, list<string>> $chunks thrift ColumnChunk bytes per flat path, one per row group
     * @param array<string, null|string> $orders thrift ColumnOrder bytes per flat path, null without column_orders
     * @param array<string, array<int, ColumnChunk>> $decoded the chunks decoded so far, per flat path
     * @param array<string, ColumnOrder> $decodedOrders
     */
    private function __construct(
        private readonly array $rowCounts,
        private readonly array $chunks,
        private readonly array $orders,
        private array $decoded = [],
        private array $decodedOrders = [],
    ) {}

    /**
     * Called by name from arrow-ext RustParquetFooter::rowGroupChunks() with the chunks its footer cache decoded.
     *
     * @param list<int> $rowCounts
     * @param array<string, list<ColumnChunk>> $chunks decoded ColumnChunks per flat path, one per row group
     * @param array<string, ColumnOrder> $orders
     */
    public static function decoded(array $rowCounts, array $chunks, array $orders): self
    {
        return new self($rowCounts, [], [], $chunks, $orders);
    }

    /**
     * Called by name from arrow-ext RustParquetFooter::rowGroupChunks(), pinned by phpt 056.
     *
     * @param list<int> $rowCounts
     * @param array<string, list<string>> $chunks
     * @param array<string, null|string> $orders
     */
    public static function fromThrift(array $rowCounts, array $chunks, array $orders): self
    {
        return new self($rowCounts, $chunks, $orders);
    }

    public function chunk(int $rowGroup, FlatColumn $column): ColumnChunk
    {
        $path = $column->flatPath();

        if (array_key_exists($rowGroup, $this->decoded[$path] ?? [])) {
            return $this->decoded[$path][$rowGroup];
        }

        if (!array_key_exists($path, $this->chunks) && !array_key_exists($path, $this->decoded)) {
            throw new InvalidArgumentException('Column "' . $path . '" was not read from the footer');
        }

        if (!array_key_exists($rowGroup, $this->chunks[$path] ?? [])) {
            throw new OutOfBoundsException('Row group ' . $rowGroup . ' does not exist');
        }

        $thrift = new ThriftColumnChunk();
        $thrift->read(new CompactProtocol(new MemoryBuffer($this->chunks[$path][$rowGroup])));

        return $this->decoded[$path][$rowGroup] = ColumnChunk::fromThrift($thrift);
    }

    public function columnOrder(FlatColumn $column): ColumnOrder
    {
        $path = $column->flatPath();

        if (array_key_exists($path, $this->decodedOrders)) {
            return $this->decodedOrders[$path];
        }

        if (!array_key_exists($path, $this->orders)) {
            throw new InvalidArgumentException('Column "' . $path . '" was not read from the footer');
        }

        $bytes = $this->orders[$path];

        if ($bytes === null) {
            return ColumnOrder::Absent;
        }

        $thrift = new ThriftColumnOrder();
        $thrift->read(new CompactProtocol(new MemoryBuffer($bytes)));

        return $this->decodedOrders[$path] = ColumnOrder::fromThrift($thrift);
    }

    public function rowCounts(): array
    {
        return $this->rowCounts;
    }
}

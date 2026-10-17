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
use function count;

final class RustRowGroupChunks implements RowGroupChunks
{
    /**
     * @var array<string, array<int, ColumnChunk>>
     */
    private array $decoded = [];

    /**
     * @var array<string, ColumnOrder>
     */
    private array $decodedOrders = [];

    /**
     * @param list<int> $rowCounts
     * @param array<string, list<string>> $chunks thrift ColumnChunk bytes per flat path, one per row group
     * @param array<string, null|string> $orders thrift ColumnOrder bytes per flat path, null without column_orders
     */
    private function __construct(
        private readonly array $rowCounts,
        private readonly array $chunks,
        private readonly array $orders,
    ) {}

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

        if (!array_key_exists($path, $this->chunks)) {
            throw new InvalidArgumentException('Column "' . $path . '" was not read from the footer');
        }

        if (!array_key_exists($rowGroup, $this->chunks[$path])) {
            throw new OutOfBoundsException('Row group ' . $rowGroup . ' does not exist');
        }

        if (!array_key_exists($rowGroup, $this->decoded[$path] ?? [])) {
            $thrift = new ThriftColumnChunk();
            $thrift->read(new CompactProtocol(new MemoryBuffer($this->chunks[$path][$rowGroup])));
            $this->decoded[$path][$rowGroup] = ColumnChunk::fromThrift($thrift);
        }

        return $this->decoded[$path][$rowGroup];
    }

    public function columnOrder(FlatColumn $column): ColumnOrder
    {
        $path = $column->flatPath();

        if (!array_key_exists($path, $this->orders)) {
            throw new InvalidArgumentException('Column "' . $path . '" was not read from the footer');
        }

        if ($this->orders[$path] === null) {
            return ColumnOrder::Absent;
        }

        if (!array_key_exists($path, $this->decodedOrders)) {
            $thrift = new ThriftColumnOrder();
            $thrift->read(new CompactProtocol(new MemoryBuffer($this->orders[$path])));
            $this->decodedOrders[$path] = ColumnOrder::fromThrift($thrift);
        }

        return $this->decodedOrders[$path];
    }

    public function count(): int
    {
        return count($this->rowCounts);
    }

    public function rowCount(int $rowGroup): int
    {
        return (
            $this->rowCounts[$rowGroup] ?? throw new OutOfBoundsException('Row group ' . $rowGroup . ' does not exist')
        );
    }
}

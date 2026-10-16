<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\RowGroup\ColumnChunk;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ThriftModel\ColumnOrder as ThriftColumnOrder;
use Flow\Parquet\ThriftModel\FileMetaData;

use function array_map;
use function array_values;
use function count;
use function sprintf;

final class Metadata
{
    /**
     * @var null|array<string, ColumnOrder>
     */
    private ?array $columnOrdersByPath = null;

    /**
     * @param list<ColumnOrder> $columnOrders one per leaf in Schema::columnsFlat() order, empty when the footer has none
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly RowGroups $rowGroups,
        private readonly int $rowsNumber,
        private readonly int $version,
        private readonly ?string $createdBy,
        private readonly array $columnOrders = [],
    ) {}

    // called by name from arrow-ext RustParquetFileReader::metadata(), pinned by phpts 047, 052
    public static function fromThrift(FileMetaData $thrift): self
    {
        $schema = Schema::fromThrift($thrift->schema);
        $columnOrders = array_map(ColumnOrder::fromThrift(...), array_values($thrift->column_orders ?? []));
        $leaves = count($schema->columnsFlat());

        if ($columnOrders !== [] && count($columnOrders) !== $leaves) {
            throw new InvalidArgumentException(sprintf(
                'Footer holds %d column orders for %d leaf columns',
                count($columnOrders),
                $leaves,
            ));
        }

        return new self(
            $schema,
            RowGroups::fromThrift($thrift->row_groups),
            (int) $thrift->num_rows,
            (int) $thrift->version,
            $thrift->created_by,
            $columnOrders,
        );
    }

    /**
     * @return array<ColumnChunk>
     */
    public function columnChunks(): array
    {
        $chunks = [];

        foreach ($this->rowGroups->all() as $rowGroup) {
            foreach ($rowGroup->columnChunks() as $columnChunk) {
                $chunks[] = $columnChunk;
            }
        }

        return $chunks;
    }

    public function columnOrder(FlatColumn $column): ColumnOrder
    {
        if ($this->columnOrders === []) {
            return ColumnOrder::Absent;
        }

        if ($this->columnOrdersByPath === null) {
            $this->columnOrdersByPath = [];

            foreach (array_values($this->schema->columnsFlat()) as $index => $leaf) {
                $this->columnOrdersByPath[$leaf->flatPath()] = $this->columnOrders[$index];
            }
        }

        return (
            $this->columnOrdersByPath[$column->flatPath()] ?? throw new InvalidArgumentException(
                'Column "' . $column->flatPath() . '" is not a leaf of the schema',
            )
        );
    }

    public function createdBy(): ?string
    {
        return $this->createdBy;
    }

    public function rowGroups(): RowGroups
    {
        return $this->rowGroups;
    }

    public function rowsNumber(): int
    {
        return $this->rowsNumber;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function toThrift(): FileMetaData
    {
        return new FileMetaData([
            'version' => $this->version,
            'schema' => $this->schema->toThrift(),
            'num_rows' => $this->rowGroups->rowsCount(),
            'row_groups' => $this->rowGroups->toThrift(),
            'created_by' => $this->createdBy,
            'column_orders' => $this->columnOrders === []
                ? null
                : array_map(
                    static fn(ColumnOrder $order): ThriftColumnOrder => $order->toThrift(),
                    $this->columnOrders,
                ),
        ]);
    }

    public function version(): int
    {
        return $this->version;
    }
}

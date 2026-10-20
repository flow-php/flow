<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Double;

use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\ParquetFile\Metadata;
use Flow\Parquet\ParquetFile\MetadataRowGroupChunks;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\RowGroupChunks;
use Flow\Parquet\ParquetFile\RowGroups;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;

/**
 * Answers from in-memory metadata, records every rowGroupChunks() call.
 */
final class SpyFooter implements Footer
{
    /**
     * @var list<list<FlatColumn>>
     */
    public array $rowGroupChunkReads = [];

    /**
     * @param list<RowGroup> $rowGroups
     * @param list<ColumnOrder> $columnOrders one per leaf of the schema, empty for a file without column_orders
     */
    public function __construct(
        private readonly Schema $schema,
        private readonly int $rows = 0,
        private readonly array $rowGroups = [],
        private readonly array $columnOrders = [],
    ) {}

    public function metadata(): Metadata
    {
        return new Metadata($this->schema, new RowGroups($this->rowGroups), $this->rows, 1, null, $this->columnOrders);
    }

    public function rowGroupChunks(array $columns): RowGroupChunks
    {
        $this->rowGroupChunkReads[] = $columns;

        return new MetadataRowGroupChunks($this->metadata());
    }

    public function rowsNumber(): int
    {
        return $this->rows;
    }

    public function schema(): Schema
    {
        return $this->schema;
    }

    public function totalByteSize(): int
    {
        return 0;
    }
}

<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnChunkBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageSizeCalculator;
use Flow\Parquet\ParquetFile\RowGroupBuilder\RowGroupContainer;
use Flow\Parquet\ParquetFile\RowGroupBuilder\RowGroupStatistics;

/**
 * @psalm-suppress PropertyNotSetInConstructor
 */
final class RowGroupBuilder
{
    /**
     * @var array<string, ColumnChunkBuilder>
     */
    private array $chunkBuilders;

    private Flattener $flattener;

    private RowGroupStatistics $statistics;

    public function __construct(
        private readonly Schema $schema,
        private readonly Compressions $compression,
        private readonly Options $options,
        private readonly DataConverter $dataConverter,
        private readonly PageSizeCalculator $calculator
    ) {
        $this->flattener = new Flattener();

        $this->chunkBuilders = $this->createColumnChunkBuilders($this->schema, $this->compression);
        $this->statistics = RowGroupStatistics::fromBuilders($this->chunkBuilders);
    }

    /**
     * @param array<string, mixed> $row
     */
    public function addRow(array $row) : void
    {
        $flatRow = [];

        foreach ($this->schema->columns() as $column) {
            $flatRow[] = $this->flattener->flattenColumn($column, $row);
        }

        foreach (\array_merge_recursive(...$flatRow) as $columnPath => $columnValues) {
            $this->chunkBuilders[$columnPath]->addRow($columnValues);
        }

        $this->statistics->addRow();
    }

    public function flush(int $fileOffset) : RowGroupContainer
    {
        $chunkContainers = [];

        foreach ($this->chunkBuilders as $chunkBuilder) {
            $chunkContainer = $chunkBuilder->flush($fileOffset);
            $fileOffset += \strlen($chunkContainer->binaryBuffer);
            $chunkContainers[] = $chunkContainer;
        }

        $buffer = '';
        $chunks = [];

        foreach ($chunkContainers as $chunkContainer) {
            $buffer .= $chunkContainer->binaryBuffer;
            $chunks[] = $chunkContainer->columnChunk;
        }

        $rowGroupContainer = new RowGroupContainer(
            $buffer,
            new RowGroup($chunks, $this->statistics->rowsCount())
        );

        $this->chunkBuilders = $this->createColumnChunkBuilders($this->schema, $this->compression);
        $this->statistics = RowGroupStatistics::fromBuilders($this->chunkBuilders);

        return $rowGroupContainer;
    }

    public function isEmpty() : bool
    {
        return $this->statistics->rowsCount() === 0;
    }

    public function isFull() : bool
    {
        return $this->statistics->uncompressedSize() >= $this->options->get(Option::ROW_GROUP_SIZE_BYTES);
    }

    public function statistics() : RowGroupStatistics
    {
        return $this->statistics;
    }

    /**
     * @return array<string, ColumnChunkBuilder>
     */
    private function createColumnChunkBuilders(Schema $schema, Compressions $compression) : array
    {
        $builders = [];

        foreach ($schema->columnsFlat() as $column) {
            $builders[$column->flatPath()] = new ColumnChunkBuilder($column, $compression, $this->dataConverter, $this->calculator, $this->options);
        }

        return $builders;
    }
}

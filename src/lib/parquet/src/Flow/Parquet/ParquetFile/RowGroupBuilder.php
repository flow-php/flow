<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnChunkBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\Flattener;
use Flow\Parquet\ParquetFile\RowGroupBuilder\RowGroupContainer;

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

    private int $rowsCount = 0;

    public function __construct(private readonly Schema $schema, private readonly DataConverter $dataConverter)
    {
        $this->flattener = new Flattener();

        $this->chunkBuilders = $this->createColumnChunkBuilders($this->schema);
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
    }

    public function flush(int $fileOffset) : RowGroupContainer
    {
        $chunkContainers = [];

        foreach ($this->chunkBuilders as $chunkBuilder) {
            foreach ($chunkBuilder->flush($fileOffset) as $chunkContainer) {
                $fileOffset += \strlen($chunkContainer->binaryBuffer);
                $chunkContainers[] = $chunkContainer;
            }
        }

        $buffer = '';
        $chunks = [];

        foreach ($chunkContainers as $chunkContainer) {
            $buffer .= $chunkContainer->binaryBuffer;
            $chunks[] = $chunkContainer->columnChunk;
        }

        $rowGroupContainer = new RowGroupContainer(
            $buffer,
            new RowGroup($chunks, $this->rowsCount)
        );

        $this->chunkBuilders = $this->createColumnChunkBuilders($this->schema);

        return $rowGroupContainer;
    }

    /**
     * @return array<string, ColumnChunkBuilder>
     */
    private function createColumnChunkBuilders(Schema $schema) : array
    {
        $builders = [];

        foreach ($schema->columnsFlat() as $column) {
            $builders[$column->flatPath()] = new ColumnChunkBuilder($column, $this->dataConverter);
        }

        return $builders;
    }
}

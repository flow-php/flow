<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\ParquetFile\Schema\{Column, NestedColumn};
use Flow\Parquet\Writer\ColumnChunkBuilder;

final class NestedColumnChunkBuilder implements ColumnChunkBuilder
{
    /** @var array<string, ColumnChunkBuilder> */
    private array $buildersByPath;

    /**
     * @param NestedColumn $column
     * @param array<ColumnChunkBuilder> $childrenColumnChunkBuilders
     */
    public function __construct(private readonly NestedColumn $column, private readonly array $childrenColumnChunkBuilders)
    {
        $this->buildersByPath = [];

        foreach ($childrenColumnChunkBuilders as $builder) {
            $this->buildersByPath[$builder->column()->flatPath()] = $builder;
        }
    }

    public function addRow(WriteColumnData $columnData) : void
    {
        foreach ($columnData->flatValues() as $flatValues) {
            $path = $flatValues->flatPath();

            if (isset($this->buildersByPath[$path])) {
                $this->buildersByPath[$path]->addRow($columnData->toFlatColumnData($path));
            }
        }
    }

    public function closePage() : void
    {
        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            $childBuilder->closePage();
        }
    }

    public function column() : Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset) : array
    {
        $offset = $fileOffset;
        $containers = [];

        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            foreach ($childBuilder->flush($offset) as $container) {
                $containers[] = $container;
                $offset += \strlen($container->binaryBuffer);
            }
        }

        return $containers;
    }

    public function isFull() : bool
    {
        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            if ($childBuilder->isFull()) {
                return true;
            }
        }

        return false;
    }

    public function uncompressedSize() : int
    {
        $size = 0;

        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            $size += $childBuilder->uncompressedSize();
        }

        return $size;
    }
}

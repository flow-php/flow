<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\WriteColumnData;
use Flow\Parquet\ParquetFile\Schema\{Column, NestedColumn};
use Flow\Parquet\Writer\ColumnChunkBuilder;

final readonly class NestedColumnChunkBuilder implements ColumnChunkBuilder
{
    /**
     * @param NestedColumn $column
     * @param array<ColumnChunkBuilder> $childrenColumnChunkBuilders
     */
    public function __construct(private NestedColumn $column, private array $childrenColumnChunkBuilders)
    {
    }

    public function addRow(WriteColumnData $columnData) : void
    {
        foreach ($columnData->flatValues() as $flatValues) {
            // We need to find the correct child column chunk builder for the flat values.
            // This is done by matching the flat path of the flat values with the child column's flat path.
            foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
                if ($childBuilder->column()->flatPath() === $flatValues->flatPath()) {
                    $childBuilder->addRow($columnData->toFlatColumnData($flatValues->flatPath()));

                    break;
                }
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

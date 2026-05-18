<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\ColumnChunkBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\Column;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Writer\ColumnChunkBuilder;

use function strlen;

final readonly class NestedColumnChunkBuilder implements ColumnChunkBuilder
{
    /**
     * @param NestedColumn $column
     * @param array<ColumnChunkBuilder> $childrenColumnChunkBuilders
     */
    public function __construct(
        private NestedColumn $column,
        private array $childrenColumnChunkBuilders,
    ) {}

    public function addColumn(WriteFlatColumnValues $columnValues): void
    {
        throw new RuntimeException(
            'NestedColumnChunkBuilder does not support addColumn(). Use flat builders directly via addColumnByFlatPath().',
        );
    }

    public function closePage(): void
    {
        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            $childBuilder->closePage();
        }
    }

    public function column(): Column
    {
        return $this->column;
    }

    public function flush(int $fileOffset): array
    {
        $offset = $fileOffset;
        $containers = [];

        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            foreach ($childBuilder->flush($offset) as $container) {
                $containers[] = $container;
                $offset += strlen($container->binaryBuffer);
            }
        }

        return $containers;
    }

    public function isFull(): bool
    {
        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            if ($childBuilder->isFull()) {
                return true;
            }
        }

        return false;
    }

    public function uncompressedSize(): int
    {
        $size = 0;

        foreach ($this->childrenColumnChunkBuilders as $childBuilder) {
            $size += $childBuilder->uncompressedSize();
        }

        return $size;
    }
}

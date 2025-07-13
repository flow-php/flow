<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\{Dremel\WriteColumnData, Options};
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn};
use Flow\Parquet\Writer\{ColumnChunkBuilder\NestedColumnChunkBuilder};

final class ColumnChunkBuilders
{
    /**
     * @param array<string, ColumnChunkBuilder> $builders
     */
    public function __construct(
        private array $builders,
    ) {
    }

    public static function initialize(Schema $schema, Options $options, Compressions $compressions) : self
    {
        $builders = [];

        foreach ($schema->columns() as $column) {
            if ($column instanceof NestedColumn) {
                $builders[$column->name()] = new NestedColumnChunkBuilder(
                    $column,
                    array_map(
                        fn (FlatColumn $childColumn) => ColumnChunkBuilderFactory::createBuilder($childColumn, $options, $compressions),
                        $column->childrenFlat()
                    )
                );
            } else {
                /** @var FlatColumn $column */
                $builders[$column->name()] = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);
            }
        }

        return new self($builders);
    }

    public function add(WriteColumnData $columnData) : void
    {
        $this->builders[$columnData->column->name()]->addRow($columnData);
    }

    /**
     * Close all pages in the column chunk builders.
     */
    public function closePages() : void
    {
        foreach ($this->builders as $builder) {
            $builder->closePage();
        }
    }

    /**
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset) : array
    {
        $offset = $fileOffset;
        $containers = [];

        foreach ($this->builders as $builder) {
            foreach ($builder->flush($offset) as $container) {
                $containers[] = $container;
                $offset += \strlen($container->binaryBuffer);
            }
        }

        return $containers;
    }

    /**
     * Check if any of the column chunk builders has reached the maximum page size.
     */
    public function isAnyPageFull() : bool
    {
        foreach ($this->builders as $builder) {
            if ($builder->isFull()) {
                return true;
            }
        }

        return false;
    }

    public function uncompressedSize() : int
    {
        $size = 0;

        foreach ($this->builders as $builder) {
            $size += $builder->uncompressedSize();
        }

        return $size;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\EfficientRowGroupBuilder;

use Flow\Parquet\{Options};
use Flow\Parquet\ParquetFile\{Compressions, Schema};
use Flow\Parquet\ParquetFile\RowGroupBuilder\{ColumnChunkContainer, WriteColumnData};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, NestedColumn, PhysicalType};

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
                        fn (FlatColumn $childColumn) => self::createFlatColumnBuilder($childColumn, $options, $compressions),
                        $column->childrenFlat()
                    )
                );
            } else {
                /** @var FlatColumn $column */
                $builders[$column->name()] = self::createFlatColumnBuilder($column, $options, $compressions);
            }
        }

        return new self($builders);
    }

    public function add(WriteColumnData $columnData) : void
    {
        $this->builders[$columnData->column->name()]->addRow($columnData);

        // Check if any builder is full and coordinate page closing across all builders
        foreach ($this->builders as $builder) {
            if ($builder->isFull()) {
                // If any builder is full, close pages on all builders to maintain synchronization
                foreach ($this->builders as $builderToClose) {
                    $builderToClose->closePage();
                }

                break;
            }
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

    public function uncompressedSize() : int
    {
        $size = 0;

        foreach ($this->builders as $builder) {
            $size += $builder->uncompressedSize();
        }

        return $size;
    }

    private static function createFlatColumnBuilder(FlatColumn $column, Options $options, Compressions $compressions) : ColumnChunkBuilder
    {
        // Use RLE_DICTIONARY encoding for boolean columns for better compression
        if ($column->type() === PhysicalType::BOOLEAN) {
            return new RLEDictionaryChunkBuilder($column, $options, $compressions);
        }

        // For other types, continue using PlainFlatColumnChunkBuilder
        return new PlainFlatColumnChunkBuilder($column, $options, $compressions);
    }
}

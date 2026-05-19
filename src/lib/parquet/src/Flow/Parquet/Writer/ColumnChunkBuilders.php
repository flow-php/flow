<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Writer\ColumnChunkBuilder\NestedColumnChunkBuilder;

use function strlen;

final class ColumnChunkBuilders
{
    /**
     * @var array<string, ColumnChunkBuilder>
     */
    private array $flatBuilders = [];

    /**
     * @param array<string, ColumnChunkBuilder> $builders
     */
    public function __construct(
        private readonly array $builders,
    ) {}

    public static function initialize(Schema $schema, Options $options, Compressions $compressions): self
    {
        $builders = [];
        $flatBuilders = [];

        foreach ($schema->columns() as $column) {
            if ($column instanceof NestedColumn) {
                $childBuilders = [];

                foreach ($column->childrenFlat() as $flatChild) {
                    $builder = ColumnChunkBuilderFactory::createBuilder($flatChild, $options, $compressions);
                    $childBuilders[$flatChild->flatPath()] = $builder;
                    $flatBuilders[$flatChild->flatPath()] = $builder;
                }

                $builders[$column->name()] = new NestedColumnChunkBuilder($column, $childBuilders);
            } else {
                /** @var FlatColumn $column */
                $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions);
                $builders[$column->name()] = $builder;
                $flatBuilders[$column->flatPath()] = $builder;
            }
        }

        $instance = new self($builders);
        $instance->flatBuilders = $flatBuilders;

        return $instance;
    }

    public function addColumnByFlatPath(string $flatPath, WriteFlatColumnValues $columnValues): void
    {
        $this->flatBuilders[$flatPath]->addColumn($columnValues);
    }

    /**
     * @return array<ColumnChunkBuilder>
     */
    public function builders(): array
    {
        return $this->builders;
    }

    /**
     * Close all pages in the column chunk builders.
     */
    public function closePages(): void
    {
        foreach ($this->builders as $builder) {
            $builder->closePage();
        }
    }

    /**
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset): array
    {
        $offset = $fileOffset;
        $containers = [];

        foreach ($this->builders as $builder) {
            foreach ($builder->flush($offset) as $container) {
                $containers[] = $container;
                $offset += strlen($container->binaryBuffer);
            }
        }

        return $containers;
    }

    /**
     * Check if any of the column chunk builders has reached the maximum page size.
     */
    public function isAnyPageFull(): bool
    {
        foreach ($this->builders as $builder) {
            if ($builder->isFull()) {
                return true;
            }
        }

        return false;
    }

    public function uncompressedSize(): int
    {
        $size = 0;

        foreach ($this->builders as $builder) {
            $size += $builder->uncompressedSize();
        }

        return $size;
    }
}

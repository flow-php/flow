<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\BloomFilter\BloomFilterOptions;
use Flow\Parquet\BloomFilter\BloomHash;
use Flow\Parquet\BloomFilter\Sbbf;
use Flow\Parquet\BloomFilter\SbbfBuilder;
use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\Schema;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\NestedColumn;
use Flow\Parquet\Writer\ColumnChunkBuilder\NestedColumnChunkBuilder;

use function array_key_exists;
use function str_repeat;
use function strlen;

final class ColumnChunkBuilders
{
    /**
     * The columns that get a bloom filter, by flat path.
     *
     * @var array<string, array{FlatColumn, BloomFilterOptions}>
     */
    private array $bloomFilterColumns = [];

    /**
     * The current row group's filters, created on a column's first non-null value.
     *
     * @var array<string, SbbfBuilder>
     */
    private array $bloomFilters = [];

    /**
     * @var array<string, ColumnChunkBuilder>
     */
    private array $flatBuilders = [];

    /**
     * @param array<string, ColumnChunkBuilder> $builders
     */
    public function __construct(
        private readonly array $builders,
        private readonly BloomHash $bloomHash = new BloomHash(),
    ) {}

    public static function initialize(Schema $schema, Options $options, Compressions $compressions): self
    {
        $builders = [];
        $flatBuilders = [];
        $statistics = StatisticsOptions::fromOptions($options);

        foreach ($schema->columns() as $column) {
            if ($column instanceof NestedColumn) {
                $childBuilders = [];

                foreach ($column->childrenFlat() as $flatChild) {
                    $builder = ColumnChunkBuilderFactory::createBuilder(
                        $flatChild,
                        $options,
                        $compressions,
                        $statistics,
                    );
                    $childBuilders[$flatChild->flatPath()] = $builder;
                    $flatBuilders[$flatChild->flatPath()] = $builder;
                }

                $builders[$column->name()] = new NestedColumnChunkBuilder($column, $childBuilders);
            } else {
                /** @var FlatColumn $column */
                $builder = ColumnChunkBuilderFactory::createBuilder($column, $options, $compressions, $statistics);
                $builders[$column->name()] = $builder;
                $flatBuilders[$column->flatPath()] = $builder;
            }
        }

        $instance = new self($builders);
        $instance->flatBuilders = $flatBuilders;
        $bloomFilters = BloomFilterOptions::forColumns($options);

        foreach ($schema->columnsFlat() as $flatColumn) {
            $flatPath = $flatColumn->flatPath();

            if (array_key_exists($flatPath, $bloomFilters)) {
                $instance->bloomFilterColumns[$flatPath] = [$flatColumn, $bloomFilters[$flatPath]];
            }
        }

        return $instance;
    }

    public function addColumnByFlatPath(string $flatPath, WriteFlatColumnValues $columnValues): void
    {
        $this->flatBuilders[$flatPath]->addColumn($columnValues);

        if (!array_key_exists($flatPath, $this->bloomFilterColumns)) {
            return;
        }

        [$column, $options] = $this->bloomFilterColumns[$flatPath];

        foreach ($columnValues->values() as $value) {
            if ($value !== null) {
                ($this->bloomFilters[$flatPath] ??= SbbfBuilder::for($options))->insert($this->bloomHash->of(
                    $column,
                    $value,
                ));
            }
        }
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
     * Each container carries its chunk's bloom filter when the column has one.
     *
     * @return array<ColumnChunkContainer>
     */
    public function flush(int $fileOffset): array
    {
        $offset = $fileOffset;
        $containers = [];

        foreach ($this->builders as $builder) {
            foreach ($builder->flush($offset) as $container) {
                $flatPath = $container->columnChunk->flatPath();
                $containers[] = array_key_exists($flatPath, $this->bloomFilterColumns)
                    ? new ColumnChunkContainer(
                        $container->binaryBuffer,
                        $container->columnChunk,
                        $container->columnIndex,
                        $container->offsetIndex,
                        ($this->bloomFilters[$flatPath] ?? null)?->build() ?? Sbbf::fromBytes(str_repeat(
                            "\0",
                            Sbbf::BLOCK_BYTES,
                        )),
                    )
                    : $container;
                $offset += strlen($container->binaryBuffer);
            }
        }

        $this->bloomFilters = [];

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

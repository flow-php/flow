<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\RowGroupContainer;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema;

use function array_chunk;
use function array_slice;
use function count;
use function min;
use function strlen;

final class RowGroupBuilder
{
    private readonly ColumnChunkBuilders $columnChunkBuilders;

    /**
     * @var array<array<array-key, mixed>>
     */
    private array $rowBuffer = [];

    private int $rowsCount = 0;

    /**
     * Rows in the pages not closed yet: every column closes its page at the same row.
     */
    private int $pageRows = 0;

    /**
     * Rows of the row groups flushed before this one: the writer's index of this group's first row.
     */
    private int $flushedRows = 0;

    private readonly int $pageMaximumRows;

    private readonly int $rowGroupSizeBytes;

    public function __construct(
        private readonly Schema $schema,
        private readonly Compressions $compression,
        private readonly Options $options,
        private readonly DremelShredder $shredder,
    ) {
        $this->pageMaximumRows = $this->options->getInt(Option::PAGE_MAXIMUM_ROWS_COUNT);

        if ($this->pageMaximumRows < 1) {
            throw new InvalidArgumentException(
                Option::PAGE_MAXIMUM_ROWS_COUNT->name . ' must be greater than 0, ' . $this->pageMaximumRows . ' given',
            );
        }

        $this->rowGroupSizeBytes = $this->options->getInt(Option::ROW_GROUP_SIZE_BYTES);

        if ($this->rowGroupSizeBytes < 1) {
            throw new InvalidArgumentException(
                Option::ROW_GROUP_SIZE_BYTES->name . ' must be greater than 0, ' . $this->rowGroupSizeBytes . ' given',
            );
        }

        $this->columnChunkBuilders = ColumnChunkBuilders::initialize($this->schema, $this->options, $this->compression);
    }

    /**
     * @param array<array-key, mixed> $row
     */
    public function addRow(array $row): void
    {
        $this->rowBuffer[] = $row;
        $this->rowsCount++;

        $interval = $this->options->getInt(Option::PAGE_SIZE_CHECK_INTERVAL);

        if (($this->rowsCount % $interval) === 0) {
            $this->flushBuffer();
        }
    }

    /**
     * @param array<array<array-key, mixed>> $rows
     */
    public function addRows(array $rows): void
    {
        /** @var int<1, max> $interval */
        $interval = $this->options->getInt(Option::PAGE_SIZE_CHECK_INTERVAL);

        // rows still buffered by addRow() came first - parquet identifies a row by its position
        $this->flushBuffer();

        foreach (array_chunk($rows, $interval) as $chunk) {
            $flatColumnsData = $this->shredder->shred($this->schema, $chunk, $this->flushedRows + $this->rowsCount);

            foreach ($flatColumnsData as $flatPath => $columnValues) {
                $this->columnChunkBuilders->addColumnByFlatPath($flatPath, $columnValues);
            }

            $this->rowsCount += count($chunk);
            $this->closeFullPages(count($chunk));
        }
    }

    /**
     * @param array<string, list<mixed>> $columns by top-level column name
     * @param int $count rows in every list
     */
    public function addColumns(array $columns, int $count): void
    {
        /** @var int<1, max> $interval */
        $interval = $this->options->getInt(Option::PAGE_SIZE_CHECK_INTERVAL);
        // rows still buffered by addRow() came first - parquet identifies a row by its position
        $this->flushBuffer();

        for ($offset = 0; $offset < $count; $offset += $interval) {
            $length = min($interval, $count - $offset);
            $slices = [];

            foreach ($columns as $name => $values) {
                $slices[$name] = array_slice($values, $offset, $length);
            }

            $flatColumnsData = $this->shredder->shredColumns(
                $this->schema,
                $slices,
                $length,
                $this->flushedRows + $this->rowsCount,
            );

            foreach ($flatColumnsData as $flatPath => $columnValues) {
                $this->columnChunkBuilders->addColumnByFlatPath($flatPath, $columnValues);
            }

            $this->rowsCount += $length;
            $this->closeFullPages($length);
        }
    }

    /**
     * The row group's column chunks, then their bloom filters (arrow-rs BloomFilterPosition::AfterRowGroup).
     */
    public function flush(int $fileOffset): RowGroupContainer
    {
        $this->flushBuffer();

        $rowsCount = $this->rowsCount();
        $offset = $fileOffset;
        $buffer = '';
        $chunks = [];
        $columnIndexes = [];
        $offsetIndexes = [];
        $this->flushedRows += $rowsCount;
        $this->rowsCount = 0;
        $this->pageRows = 0;

        $bloomFilters = [];

        foreach ($this->columnChunkBuilders->flush($offset) as $container) {
            $chunks[] = $container->columnChunk;
            $columnIndexes[] = $container->columnIndex;
            $offsetIndexes[] = $container->offsetIndex;
            $bloomFilters[] = $container->bloomFilter;
            $buffer .= $container->binaryBuffer;
        }

        foreach ($bloomFilters as $index => $bloomFilter) {
            if ($bloomFilter === null) {
                continue;
            }

            $bytes = $bloomFilter->serialized();
            $chunks[$index] = $chunks[$index]->withLocations(
                bloomFilterOffset: $fileOffset + strlen($buffer),
                bloomFilterLength: strlen($bytes),
            );
            $buffer .= $bytes;
        }

        return new RowGroupContainer($buffer, new RowGroup($chunks, $rowsCount), $columnIndexes, $offsetIndexes);
    }

    public function isEmpty(): bool
    {
        return $this->rowsCount() === 0;
    }

    public function isFull(): bool
    {
        return $this->columnChunkBuilders->uncompressedSize() >= $this->rowGroupSizeBytes;
    }

    public function rowsCount(): int
    {
        return $this->rowsCount;
    }

    private function flushBuffer(): void
    {
        if (count($this->rowBuffer) === 0) {
            return;
        }

        $flatColumnsData = $this->shredder->shred(
            $this->schema,
            $this->rowBuffer,
            $this->flushedRows + $this->rowsCount - count($this->rowBuffer),
        );

        foreach ($flatColumnsData as $flatPath => $columnValues) {
            $this->columnChunkBuilders->addColumnByFlatPath($flatPath, $columnValues);
        }

        $this->closeFullPages(count($this->rowBuffer));
        $this->rowBuffer = [];
    }

    /**
     * Pages close once one reaches PAGE_SIZE_BYTES or they hold PAGE_MAXIMUM_ROWS_COUNT rows, checked every
     * PAGE_SIZE_CHECK_INTERVAL rows.
     */
    private function closeFullPages(int $rows): void
    {
        $this->pageRows += $rows;

        if ($this->columnChunkBuilders->isAnyPageFull() || $this->pageRows >= $this->pageMaximumRows) {
            $this->columnChunkBuilders->closePages();
            $this->pageRows = 0;
        }
    }
}

<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Dremel\DremelShredder;
use Flow\Parquet\Dremel\RowGroupContainer;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Compressions;
use Flow\Parquet\ParquetFile\RowGroup;
use Flow\Parquet\ParquetFile\Schema;

use function array_chunk;
use function count;

final class RowGroupBuilder
{
    private readonly ColumnChunkBuilders $columnChunkBuilders;

    /**
     * @var array<array<string, mixed>>
     */
    private array $rowBuffer = [];

    private int $rowsCount = 0;

    public function __construct(
        private readonly Schema $schema,
        private readonly Compressions $compression,
        private readonly Options $options,
        private readonly DremelShredder $shredder,
    ) {
        $this->columnChunkBuilders = ColumnChunkBuilders::initialize($this->schema, $this->options, $this->compression);
    }

    /**
     * @param array<string, mixed> $row
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
     * @param array<array<string, mixed>> $rows
     */
    public function addRows(array $rows): void
    {
        /** @var int<1, max> $interval */
        $interval = $this->options->getInt(Option::PAGE_SIZE_CHECK_INTERVAL);

        // rows still buffered by addRow() came first - parquet identifies a row by its position
        $this->flushBuffer();

        foreach (array_chunk($rows, $interval) as $chunk) {
            $flatColumnsData = $this->shredder->shred($this->schema, $chunk);

            foreach ($flatColumnsData as $flatPath => $columnValues) {
                $this->columnChunkBuilders->addColumnByFlatPath($flatPath, $columnValues);
            }

            $this->rowsCount += count($chunk);

            if ($this->columnChunkBuilders->isAnyPageFull()) {
                $this->columnChunkBuilders->closePages();
            }
        }
    }

    public function flush(int $fileOffset): RowGroupContainer
    {
        $this->flushBuffer();

        $rowsCount = $this->rowsCount();
        $offset = $fileOffset;
        $buffer = '';
        $chunks = [];
        $this->rowsCount = 0;

        foreach ($this->columnChunkBuilders->flush($offset) as $container) {
            $chunks[] = $container->columnChunk;
            $buffer .= $container->binaryBuffer;
        }

        return new RowGroupContainer($buffer, new RowGroup($chunks, $rowsCount));
    }

    public function isEmpty(): bool
    {
        return $this->rowsCount() === 0;
    }

    public function isFull(): bool
    {
        return $this->columnChunkBuilders->uncompressedSize() >= $this->options->getInt(Option::ROW_GROUP_SIZE_BYTES);
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

        $flatColumnsData = $this->shredder->shred($this->schema, $this->rowBuffer);

        foreach ($flatColumnsData as $flatPath => $columnValues) {
            $this->columnChunkBuilders->addColumnByFlatPath($flatPath, $columnValues);
        }

        if ($this->columnChunkBuilders->isAnyPageFull()) {
            $this->columnChunkBuilders->closePages();
        }

        $this->rowBuffer = [];
    }
}

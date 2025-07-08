<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer;

use Flow\Parquet\Dremel\{DremelShredder, RowGroupContainer};
use Flow\Parquet\{Option, Options, ParquetFile\Compressions, ParquetFile\RowGroup, ParquetFile\Schema};

final class RowGroupBuilder
{
    private readonly ColumnChunkBuilders $columnChunkBuilders;

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
    public function addRow(array $row) : void
    {
        foreach ($this->schema->columns() as $column) {
            $this->columnChunkBuilders->add($this->shredder->shred($column, $row));
        }

        $this->rowsCount++;
        $interval = $this->options->getInt(Option::PAGE_SIZE_CHECK_INTERVAL);

        if (($this->rowsCount % $interval === 0) && $this->columnChunkBuilders->isAnyPageFull()) {
            $this->columnChunkBuilders->closePages();
        }
    }

    public function flush(int $fileOffset) : RowGroupContainer
    {
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

    public function isEmpty() : bool
    {
        return $this->rowsCount() === 0;
    }

    public function isFull() : bool
    {
        return $this->columnChunkBuilders->uncompressedSize() >= $this->options->getInt(Option::ROW_GROUP_SIZE_BYTES);
    }

    public function rowsCount() : int
    {
        return $this->rowsCount;
    }
}

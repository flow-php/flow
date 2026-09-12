<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\Writer;
use Flow\Filesystem\DestinationStream;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFileWriter;

use function array_fill_keys;

final class ArrowParquetFileWriter implements ParquetFileWriter
{
    /**
     * @var array<string, list<mixed>>
     */
    private array $batch;

    private int $batchSize = 0;

    private ?Writer $arrowWriter;

    /**
     * @param list<string> $columnNames
     */
    public function __construct(
        Writer $arrowWriter,
        private readonly DestinationStream $stream,
        private readonly array $columnNames,
        private readonly int $batchSizeLimit,
    ) {
        $this->arrowWriter = $arrowWriter;
        $this->batch = array_fill_keys($this->columnNames, []);
    }

    public function close(): void
    {
        $writer = $this->arrowWriter ?? throw new RuntimeException('Writer is not open');

        try {
            if ($this->batchSize > 0) {
                $writer->writeBatch($this->batch);
            }

            $writer->close();
            $this->stream->close();
        } finally {
            $this->arrowWriter = null;
        }
    }

    public function writeBatch(iterable $rows): void
    {
        foreach ($rows as $row) {
            $this->writeRow($row);
        }
    }

    public function writeRow(array $row): void
    {
        $writer = $this->arrowWriter ?? throw new RuntimeException('Writer is not open');

        foreach ($this->columnNames as $name) {
            $this->batch[$name][] = $row[$name] ?? null;
        }

        $this->batchSize++;

        if ($this->batchSize >= $this->batchSizeLimit) {
            $writer->writeBatch($this->batch);
            $this->batch = array_fill_keys($this->columnNames, []);
            $this->batchSize = 0;
        }
    }
}

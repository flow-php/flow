<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Arrow\Parquet\RowsWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFileWriter;

use function array_values;
use function is_array;

final class ArrowParquetFileWriter implements ParquetFileWriter
{
    private ?RowsWriter $writer;

    public function __construct(RowsWriter $writer)
    {
        $this->writer = $writer;
    }

    public function close(): void
    {
        $writer = $this->writer ?? throw new RuntimeException('Writer is not open');

        try {
            $writer->close();
        } finally {
            $this->writer = null;
        }
    }

    public function writeBatch(iterable $rows): void
    {
        $writer = $this->writer ?? throw new RuntimeException('Writer is not open');

        if (is_array($rows)) {
            $writer->writeRows(array_values($rows));

            return;
        }

        foreach ($rows as $row) {
            $writer->writeRow($row);
        }
    }

    public function writeColumns(array $columns): void
    {
        ($this->writer ?? throw new RuntimeException('Writer is not open'))->writeColumns($columns);
    }

    public function writeRow(array $row): void
    {
        ($this->writer ?? throw new RuntimeException('Writer is not open'))->writeRow($row);
    }
}

<?php

declare(strict_types=1);

namespace Flow\Parquet\Engine;

use Flow\Parquet\Engine\Native\NativeParquetRowsWriter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFileWriter;

use function array_values;
use function is_array;

final class NativeParquetFileWriter implements ParquetFileWriter
{
    private ?NativeParquetRowsWriter $writer;

    public function __construct(NativeParquetRowsWriter $writer)
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

    public function writeRow(array $row): void
    {
        ($this->writer ?? throw new RuntimeException('Writer is not open'))->writeRow($row);
    }
}

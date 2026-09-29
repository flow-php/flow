<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;

final readonly class NativeParquetOpenSink implements ParquetOpenSink
{
    public function __construct(
        private NativeParquetWriter $writer,
    ) {}

    public function close(): void
    {
        $this->writer->close();
    }

    public function write(Rows $rows): void
    {
        $this->writer->write($rows);
    }
}

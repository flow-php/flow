<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;
use Flow\Parquet\ParquetFileWriter;

final readonly class PhpParquetOpenSink implements ParquetOpenSink
{
    public function __construct(
        private ParquetFileWriter $file,
        private ParquetEncoder $encoder,
    ) {}

    public function close(): void
    {
        $this->file->close();
    }

    public function write(Rows $rows): void
    {
        $this->file->writeColumns($this->encoder->columns($rows));
    }
}

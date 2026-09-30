<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;
use Flow\Parquet\Writer;

final readonly class EngineParquetOpenSink implements ParquetOpenSink
{
    public function __construct(
        private Writer $writer,
        private ParquetEncoder $encoder,
    ) {}

    public function close(): void
    {
        $this->writer->close();
    }

    public function write(Rows $rows): void
    {
        $this->writer->writeColumns($this->encoder->columns($rows));
    }
}

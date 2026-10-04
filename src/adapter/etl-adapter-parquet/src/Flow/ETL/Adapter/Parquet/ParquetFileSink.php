<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;

final class ParquetFileSink implements FileSink
{
    private ?AdaptiveParquetOpenSink $writer = null;

    public function __construct(
        private readonly ParquetFileSinks $sinks,
        private readonly DestinationStream $stream,
        private readonly Backend $backend,
    ) {}

    public function close(): void
    {
        $this->writer?->close();
    }

    public function write(Rows $rows): void
    {
        $conformed = $this->sinks->conform($rows, $this->backend);

        ($this->writer ??= $this->sinks->writer($this->stream))->write($conformed);
    }
}

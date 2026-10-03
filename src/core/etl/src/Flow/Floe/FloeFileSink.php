<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;

final class FloeFileSink implements FileSink
{
    private ?FloeWriter $writer = null;

    public function __construct(
        private readonly FloeFileSinks $sinks,
        private readonly DestinationStream $stream,
        private readonly Backend $backend,
    ) {}

    public function close(): void
    {
        $this->writer?->close();
    }

    public function write(Rows $rows): void
    {
        ($this->writer ??= $this->sinks->writer($this->stream, $rows->schema(), $this->backend))->write($rows);
    }
}

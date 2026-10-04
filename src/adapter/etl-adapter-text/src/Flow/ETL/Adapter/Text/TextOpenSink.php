<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text;

use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;

final readonly class TextOpenSink implements FileSink
{
    public function __construct(
        private DestinationStream $stream,
        private TextEncoder $encoder,
    ) {}

    /**
     * The stream belongs to the FilesSink that opened it.
     */
    public function close(): void {}

    public function write(Rows $rows): void
    {
        $this->stream->append($this->encoder->encode($rows));
    }
}

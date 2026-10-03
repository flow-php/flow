<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV;

use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Rows;
use Flow\Filesystem\DestinationStream;

use function array_values;

final class CSVOpenSink implements FileSink
{
    private bool $written = false;

    public function __construct(
        private readonly DestinationStream $stream,
        private readonly CSVEncoder $encoder,
        private readonly bool $header,
    ) {}

    /**
     * The stream belongs to the FilesSink that opened it.
     */
    public function close(): void {}

    public function write(Rows $rows): void
    {
        $this->stream->append(
            (
                $this->header && !$this->written
                    ? $this->encoder->encodeHeader(array_values($rows->schema()->references()->names()))
                    : ''
            )
                . $this->encoder->encode($rows),
        );
        $this->written = true;
    }
}

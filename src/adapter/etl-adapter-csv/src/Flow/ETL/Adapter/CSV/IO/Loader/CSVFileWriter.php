<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\CSV\IO\Loader;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\DestinationStream;

use function array_values;

final class CSVFileWriter implements FileWriter
{
    private bool $written = false;

    public function __construct(
        private readonly DestinationStream $stream,
        private readonly CSVEncoder $encoder,
        private readonly bool $header,
    ) {}

    /**
     * The stream belongs to the FileCommit that opened it.
     */
    public function close(): void {}

    public function write(Rows $rows): void
    {
        $this->stream->append(
            (
                $this->header && !$this->written
                    ? $this->encoder->encodeHeader(array_values($rows->schema()->names()))
                    : ''
            )
                . $this->encoder->encode($rows),
        );
        $this->written = true;
    }
}

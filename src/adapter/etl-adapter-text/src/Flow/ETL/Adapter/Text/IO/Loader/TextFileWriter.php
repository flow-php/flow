<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Text\IO\Loader;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Values\Rows;
use Flow\Filesystem\DestinationStream;

final readonly class TextFileWriter implements FileWriter
{
    public function __construct(
        private DestinationStream $stream,
        private TextEncoder $encoder,
    ) {}

    /**
     * The stream belongs to the FileCommit that opened it.
     */
    public function close(): void {}

    public function write(Rows $rows): void
    {
        $this->stream->append($this->encoder->encode($rows));
    }
}

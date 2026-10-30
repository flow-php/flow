<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Floe;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\Storage\Floe\FloeStreamWriter;
use Flow\ETL\Values\Rows;

final readonly class FloeFileWriter implements FileWriter
{
    public function __construct(
        private FloeStreamWriter $writer,
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

<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Loader\File;

use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Flow\Filesystem\DestinationStream;

interface FileWriterFactory
{
    /**
     * Opens the writer of a stream the write has not written to yet.
     *
     * @param Schema $schema the schema of the stream's first batch
     * @param Backend $backend the configured backend of the write
     */
    public function writer(DestinationStream $stream, Schema $schema, Backend $backend): FileWriter;
}

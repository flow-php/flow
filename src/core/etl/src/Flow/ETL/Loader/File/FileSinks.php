<?php

declare(strict_types=1);

namespace Flow\ETL\Loader\File;

use Flow\ETL\Column\Backend;
use Flow\Filesystem\DestinationStream;

interface FileSinks
{
    /**
     * Opens the sink of a stream the write has not written to yet.
     *
     * @param Backend $backend the configured backend of the write
     */
    public function open(DestinationStream $stream, Backend $backend): FileSink;
}

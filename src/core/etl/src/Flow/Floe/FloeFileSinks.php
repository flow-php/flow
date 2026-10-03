<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\ETL\Loader\File\FileSink;
use Flow\ETL\Loader\File\FileSinks;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;

final class FloeFileSinks implements FileSinks
{
    private ?Schema $schema;

    /**
     * @param null|Schema $declared the schema every file is written under, partition columns removed; null to take
     *                              the first written batch's schema
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        ?Schema $declared,
        private readonly ?Metadata $metadata,
        private readonly Options $options,
    ) {
        $this->schema = $declared;
    }

    public function open(DestinationStream $stream, Backend $backend): FileSink
    {
        return new FloeFileSink($this, $stream, $backend);
    }

    /**
     * The writer of one stream under the run's schema - the declared one, or the first written batch's.
     */
    public function writer(DestinationStream $stream, Schema $written, Backend $backend): FloeWriter
    {
        $writer = new FloeWriter($this->filesystem, $this->schema ??= $written, $backend, $this->options);
        $writer->createForStream($stream, $this->metadata);

        return $writer;
    }
}

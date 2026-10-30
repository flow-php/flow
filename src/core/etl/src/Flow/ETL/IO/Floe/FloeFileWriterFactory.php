<?php

declare(strict_types=1);

namespace Flow\ETL\IO\Floe;

use Flow\ETL\IO\Loader\File\FileWriter;
use Flow\ETL\IO\Loader\File\FileWriterFactory;
use Flow\ETL\Storage\Floe\FloeStreamWriter;
use Flow\ETL\Storage\Floe\Options;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Filesystem\DestinationStream;

final class FloeFileWriterFactory implements FileWriterFactory
{
    private ?Schema $schema;

    /**
     * @param null|Schema $declared the schema every file is written under, partition columns removed; null to take
     *                              the first written batch's schema
     */
    public function __construct(
        ?Schema $declared,
        private readonly ?Metadata $metadata,
        private readonly Options $options,
    ) {
        $this->schema = $declared;
    }

    /**
     * The writer of one stream under the run's schema - the declared one, or the first opened stream's.
     */
    public function writer(DestinationStream $stream, Schema $schema, Backend $backend): FileWriter
    {
        return new FloeFileWriter(FloeStreamWriter::create(
            $stream,
            $this->schema ??= $schema,
            $backend,
            $this->options,
            $this->metadata,
        ));
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Storage\Floe;

use Flow\ETL\Storage\Floe\Exception\IncompatibleSchemaException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Frame\FloeException;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Metadata;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;

use function sprintf;

final readonly class FloeFiles
{
    public function __construct(
        private Filesystem $filesystem,
        private Backend $backend,
        private Options $options = new Options(),
        private int $chunkSize = 65536,
    ) {}

    /**
     * @param ?Metadata $metadata merged over the existing footer metadata, new keys win
     *
     * @throws FloeException
     * @throws IncompatibleSchemaException
     */
    public function append(Path $path, Schema $schema, ?Metadata $metadata = null): FloeStreamWriter
    {
        if ($this->filesystem->status($path) === null) {
            return $this->create($path, $schema, $metadata);
        }

        $source = $this->filesystem->readFrom($path);
        $size = $source->size();

        if ($size === null) {
            $source->close();

            throw new FloeException(sprintf(
                'Floe append requires a sized stream, "%s" does not report its size',
                $path->uri(),
            ));
        }

        if ($size === 0) {
            $source->close();

            return $this->create($path, $schema, $metadata);
        }

        $location = (new FooterReader())->read($source, $this->options->codec);
        $source->close();

        return FloeStreamWriter::resume(
            $this->filesystem->appendTo($path),
            $location->footer,
            $size,
            $schema,
            $this->backend,
            $this->options,
            $metadata,
        );
    }

    /**
     * @param Schema $schema fixes the session schema for the writer's life
     *
     * @throws FloeException
     */
    public function create(Path $path, Schema $schema, ?Metadata $metadata = null): FloeStreamWriter
    {
        return FloeStreamWriter::create(
            $this->filesystem->writeTo($path),
            $schema,
            $this->backend,
            $this->options,
            $metadata,
        );
    }

    /**
     * @throws FloeException
     */
    public function read(Path $path): FloeStreamReader
    {
        return new FloeStreamReader(
            $this->filesystem->readFrom($path),
            $this->options->codec,
            $this->chunkSize,
            $this->backend,
        );
    }
}

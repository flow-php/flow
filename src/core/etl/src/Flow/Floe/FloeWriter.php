<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Hydrator;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\ETL\Schema\Metadata;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Exception\FloeException;

use function sprintf;

final class FloeWriter
{
    private readonly FloeStreamWriter $inner;

    /**
     * @param Schema $schema fixes the session schema for the writer's life
     * @param null|Hydrator $hydrator null uses the adaptive hydrator
     *
     * @throws FloeException
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        Schema $schema,
        private readonly Options $options = new Options(),
        ?Hydrator $hydrator = null,
        FloeEngine $engine = FloeEngine::adaptive,
    ) {
        $this->inner = new FloeStreamWriter($schema, $this->options, $hydrator, $engine);
    }

    /**
     * @param ?Metadata $metadata merged over the existing footer metadata, new keys win
     *
     * @throws FloeException
     */
    public function append(Path $path, ?Metadata $metadata = null): void
    {
        if ($this->filesystem->status($path) === null) {
            $this->create($path, $metadata);

            return;
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
            $this->create($path, $metadata);

            return;
        }

        $location = (new FooterReader())->read($source, $this->options->codec);
        $source->close();

        $this->inner->resume($this->filesystem->appendTo($path), $location->footer, $size, $metadata);
    }

    /**
     * @throws FloeException
     */
    public function close(): void
    {
        $this->inner->close();
    }

    /**
     * @param ?Metadata $metadata stored in the footer
     *
     * @throws FloeException
     */
    public function create(Path $path, ?Metadata $metadata = null): void
    {
        $this->inner->create($this->filesystem->writeTo($path), $metadata);
    }

    /**
     * @param ?Metadata $metadata stored in the footer
     *
     * @throws FloeException
     */
    public function createForStream(DestinationStream $stream, ?Metadata $metadata = null): void
    {
        $this->inner->create($stream, $metadata);
    }

    /**
     * @throws FloeException
     * @throws Exception\IncompatibleSchemaException
     */
    public function write(Rows $rows): void
    {
        $this->inner->write($rows);
    }
}

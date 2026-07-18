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
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;

use function sprintf;

final class FloeWriter
{
    private readonly FloeStreamWriter $inner;

    /**
     * @param null|Hydrator $hydrator null uses the adaptive hydrator
     *
     * @throws FloeException
     */
    public function __construct(
        private readonly Filesystem $filesystem,
        private readonly Codec $codec = new NoopCodec(),
        ?Hydrator $hydrator = null,
        int $bufferSize = 65_536,
    ) {
        $this->inner = new FloeStreamWriter($this->codec, $hydrator, $bufferSize);
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

        $location = (new FooterReader())->read($source, $this->codec);
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
     * @param ?Schema $schema fixes the session schema; null derives it from the first batch
     *
     * @throws FloeException
     */
    public function create(Path $path, ?Metadata $metadata = null, ?Schema $schema = null): void
    {
        $this->inner->create($this->filesystem->writeTo($path), $metadata, $schema);
    }

    /**
     * @param ?Metadata $metadata stored in the footer
     * @param ?Schema $schema fixes the session schema; null derives it from the first batch
     *
     * @throws FloeException
     */
    public function createForStream(DestinationStream $stream, ?Metadata $metadata = null, ?Schema $schema = null): void
    {
        $this->inner->create($stream, $metadata, $schema);
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

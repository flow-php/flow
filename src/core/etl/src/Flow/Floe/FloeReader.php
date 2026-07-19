<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Row\Hydrator;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;

final readonly class FloeReader
{
    /**
     * @param null|Hydrator $hydrator null uses the adaptive hydrator
     */
    public function __construct(
        private Filesystem $filesystem,
        private Codec $codec = new NoopCodec(),
        private int $chunkSize = 65536,
        private ?Hydrator $hydrator = null,
        private FloeEngine $engine = FloeEngine::adaptive,
    ) {}

    /**
     * @throws FloeException
     */
    public function read(Path $path): FloeStreamReader
    {
        return new FloeStreamReader(
            $this->filesystem->readFrom($path),
            $this->codec,
            $this->chunkSize,
            $this->hydrator,
            $this->engine,
        );
    }
}

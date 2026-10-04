<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\ETL\Column\Backend;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;

final readonly class FloeReader
{
    public function __construct(
        private Filesystem $filesystem,
        private Backend $backend,
        private Codec $codec = new NoopCodec(),
        private int $chunkSize = 65536,
    ) {}

    /**
     * @throws FloeException
     */
    public function read(Path $path): FloeStreamReader
    {
        return new FloeStreamReader($this->filesystem->readFrom($path), $this->codec, $this->chunkSize, $this->backend);
    }
}

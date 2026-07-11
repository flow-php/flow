<?php

declare(strict_types=1);

namespace Flow\Floe;

use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Flow\Floe\Codec\NoopCodec;
use Flow\Floe\Exception\FloeException;

final readonly class FloeReader
{
    /**
     * @param null|bool $useExtension null auto-detects the flow_php extension; the explicit flag exists for parity tests
     *
     * @throws FloeException
     */
    public function __construct(
        private Filesystem $filesystem,
        private Codec $codec = new NoopCodec(),
        private int $chunkSize = 65536,
        private ?bool $useExtension = null,
    ) {
        Format::validateCodecId($this->codec->id());
    }

    public function read(Path $path): FloeFile
    {
        return new FloeFile(
            fn(): SourceStream => $this->filesystem->readFrom($path),
            $path->uri(),
            $this->codec,
            $this->chunkSize,
            $this->useExtension,
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;

use function sprintf;

final readonly class FailingAppendDestinationStream implements DestinationStream
{
    public function __construct(
        private DestinationStream $inner,
    ) {}

    public function append(string $data): self
    {
        throw new RuntimeException(sprintf('Appending to "%s" failed', $this->inner->path()->uri()));
    }

    public function close(): void
    {
        $this->inner->close();
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource): self
    {
        $this->inner->fromResource($resource);

        return $this;
    }

    public function isOpen(): bool
    {
        return $this->inner->isOpen();
    }

    public function path(): Path
    {
        return $this->inner->path();
    }
}

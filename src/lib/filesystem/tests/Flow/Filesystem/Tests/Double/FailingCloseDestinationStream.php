<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;

use function sprintf;

final class FailingCloseDestinationStream implements DestinationStream
{
    private bool $closed = false;

    public function __construct(
        private readonly DestinationStream $inner,
    ) {}

    public function append(string $data): self
    {
        $this->inner->append($data);

        return $this;
    }

    public function close(): void
    {
        // the handle is released first - a real failed close leaves nothing to retry
        $this->closed = true;
        $this->inner->close();

        throw new RuntimeException(sprintf('Closing "%s" failed', $this->inner->path()->uri()));
    }

    public function fromResource($resource): self
    {
        $this->inner->fromResource($resource);

        return $this;
    }

    public function isOpen(): bool
    {
        return !$this->closed && $this->inner->isOpen();
    }

    public function path(): Path
    {
        return $this->inner->path();
    }
}

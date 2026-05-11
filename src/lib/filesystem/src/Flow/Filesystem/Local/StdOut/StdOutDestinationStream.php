<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local\StdOut;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;

final class StdOutDestinationStream implements DestinationStream
{
    /**
     * @var closed-resource|resource
     */
    private $handle;

    /**
     * @param 'output'|'stderr'|'stdout' $target
     */
    public function __construct(
        private readonly Path $path,
        string $target = 'stdout',
        ?\php_user_filter $filter = null,
        private readonly ?\Closure $onClose = null,
    ) {
        if ($filter !== null) {
            stream_filter_register($filter::class, $filter::class);
            /** @phpstan-ignore-next-line */
            $this->handle = fopen('php://' . $target, 'wb');
            /** @phpstan-ignore-next-line */
            stream_filter_append($this->handle, $filter::class);
        } else {
            /** @phpstan-ignore-next-line */
            $this->handle = fopen('php://' . $target, 'wb');
        }
    }

    public function append(string $data): DestinationStream
    {
        if (\is_resource($this->handle)) {
            $written = \fwrite($this->handle, $data);

            if ($written === false || $written !== \strlen($data)) {
                throw new RuntimeException(
                    'Failed to write all bytes to stream, expected '
                    . \strlen($data)
                    . ' bytes, written: '
                    . ($written === false ? '0' : $written),
                );
            }
        }

        return $this;
    }

    public function close(): void
    {
        if (\is_resource($this->handle)) {
            \fclose($this->handle);
        }

        if ($this->onClose !== null) {
            ($this->onClose)();
        }
    }

    public function fromResource($resource): DestinationStream
    {
        if (\is_resource($this->handle)) {
            stream_copy_to_stream($resource, $this->handle);
        }

        return $this;
    }

    public function isOpen(): bool
    {
        return \is_resource($this->handle);
    }

    public function path(): Path
    {
        return $this->path;
    }
}

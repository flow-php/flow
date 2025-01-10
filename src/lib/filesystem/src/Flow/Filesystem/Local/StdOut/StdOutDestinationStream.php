<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local\StdOut;

use Flow\Filesystem\{DestinationStream, Exception\InvalidArgumentException, Path};

final class StdOutDestinationStream implements DestinationStream
{
    /**
     * @var closed-resource|resource
     */
    private $handle;

    public function __construct(private readonly Path $path, ?\php_user_filter $filter = null)
    {
        $outputStream = \mb_strtolower((string) $this->path->options()->getAsString('stream', 'stdout'));

        if (!\in_array($outputStream, ['stdout', 'stderr', 'output'], true)) {
            throw new InvalidArgumentException('Invalid output stream, allowed values are "stdout", "stderr" and "output", given: ' . $outputStream);
        }

        if ($filter !== null) {
            stream_filter_register($outputStream, $filter::class);
            /** @phpstan-ignore-next-line */
            $this->handle = fopen('php://' . $outputStream, 'wb');
            /** @phpstan-ignore-next-line */
            stream_filter_append($this->handle, 'stdout');
        } else {
            /** @phpstan-ignore-next-line */
            $this->handle = fopen('php://' . $outputStream, 'wb');
        }
    }

    public function append(string $data) : DestinationStream
    {
        if (\is_resource($this->handle)) {
            fwrite($this->handle, $data);
        }

        return $this;
    }

    public function close() : void
    {
        if (\is_resource($this->handle)) {
            \fclose($this->handle);
        }
    }

    public function fromResource($resource) : DestinationStream
    {
        if (\is_resource($this->handle)) {
            stream_copy_to_stream($resource, $this->handle);
        }

        return $this;
    }

    public function isOpen() : bool
    {
        return \is_resource($this->handle);
    }

    public function path() : Path
    {
        return $this->path;
    }
}

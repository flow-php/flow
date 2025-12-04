<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local\Memory;

use Flow\Filesystem\{DestinationStream, Exception\InvalidArgumentException, Path, SourceStream};

final class MemoryStream implements DestinationStream, SourceStream
{
    /**
     * @param resource $handle
     */
    public function __construct(private $handle, private readonly Path $path)
    {
        if (!\is_resource($this->handle)) {
            throw new InvalidArgumentException('Invalid memory stream handle');
        }
    }

    public function __destruct()
    {
        if (\is_resource($this->handle)) {
            \fclose($this->handle);
        }
    }

    #[\Override]
    public function append(string $data) : DestinationStream
    {
        fwrite($this->handle, $data);

        return $this;
    }

    /**
     * We are not closing memory streams, in order to cleanup memory use rm on Memory Filesystem.
     */
    #[\Override]
    public function close() : void
    {
    }

    #[\Override]
    public function content() : string
    {
        \fseek($this->handle, 0);

        $content = \stream_get_contents($this->handle);

        if ($content === false) {
            throw new InvalidArgumentException('Failed to read memory stream content');
        }

        return $content;
    }

    #[\Override]
    public function fromResource($resource) : DestinationStream
    {
        stream_copy_to_stream($resource, $this->handle);

        return $this;
    }

    #[\Override]
    public function isOpen() : bool
    {
        return true;
    }

    #[\Override]
    public function iterate(int $length = 1) : \Generator
    {
        \fseek($this->handle, 0);

        while (!\feof($this->handle)) {
            yield (string) \fread($this->handle, $length);
        }
    }

    #[\Override]
    public function path() : Path
    {
        return $this->path;
    }

    #[\Override]
    public function read(int $length, int $offset) : string
    {
        \fseek($this->handle, $offset);

        return (string) \fread($this->handle, $length);
    }

    #[\Override]
    public function readLines(string $separator = "\n", ?int $length = null) : \Generator
    {
        \fseek($this->handle, 0);

        while (!\feof($this->handle)) {
            yield (string) \fgets($this->handle, $length);
        }
    }

    #[\Override]
    public function size() : ?int
    {
        $stat = \fstat($this->handle);

        return $stat['size'] ?? null;
    }
}

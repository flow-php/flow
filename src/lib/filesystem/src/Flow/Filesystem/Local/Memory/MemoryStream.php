<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local\Memory;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SourceStream;
use Generator;

use function fclose;
use function feof;
use function fread;
use function fseek;
use function fstat;
use function fwrite;
use function is_resource;
use function stream_get_contents;
use function stream_get_line;
use function strlen;

use const PHP_INT_MAX;

final class MemoryStream implements DestinationStream, SourceStream
{
    /**
     * @param resource $handle
     */
    public function __construct(
        private $handle,
        private readonly Path $path,
    ) {
        if (!is_resource($this->handle)) {
            throw new InvalidArgumentException('Invalid memory stream handle');
        }
    }

    public function __destruct()
    {
        if (is_resource($this->handle)) {
            fclose($this->handle);
        }
    }

    public function append(string $data): DestinationStream
    {
        fseek($this->handle, 0, SEEK_END);
        $written = fwrite($this->handle, $data);

        if ($written === false || $written !== strlen($data)) {
            throw new RuntimeException(
                'Failed to write all bytes to stream, expected '
                . strlen($data)
                . ' bytes, written: '
                . ($written === false ? '0' : $written),
            );
        }

        return $this;
    }

    /**
     * We are not closing memory streams, in order to cleanup memory use rm on Memory Filesystem.
     */
    public function close(): void {}

    public function content(): string
    {
        fseek($this->handle, 0);

        $content = stream_get_contents($this->handle);

        if ($content === false) {
            throw new InvalidArgumentException('Failed to read memory stream content');
        }

        return $content;
    }

    public function fromResource($resource): DestinationStream
    {
        fseek($this->handle, 0, SEEK_END);
        stream_copy_to_stream($resource, $this->handle, null);

        return $this;
    }

    public function isOpen(): bool
    {
        return true;
    }

    public function iterate(int $length = 1): Generator
    {
        fseek($this->handle, 0);

        while (!feof($this->handle)) {
            yield (string) fread($this->handle, $length);
        }
    }

    public function path(): Path
    {
        return $this->path;
    }

    public function read(int $length, int $offset): string
    {
        fseek($this->handle, $offset);

        return (string) fread($this->handle, $length);
    }

    public function readLines(string $separator = "\n", ?int $length = null): Generator
    {
        fseek($this->handle, 0);

        while (!feof($this->handle)) {
            $line = stream_get_line($this->handle, PHP_INT_MAX, $separator);

            if ($line === false) {
                break;
            }

            yield $line;
        }
    }

    public function size(): ?int
    {
        $stat = fstat($this->handle);

        return $stat['size'] ?? null;
    }
}

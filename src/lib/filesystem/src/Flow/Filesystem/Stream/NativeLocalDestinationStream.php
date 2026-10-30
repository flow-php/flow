<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;

use function error_get_last;
use function fclose;
use function file_exists;
use function fopen;
use function fseek;
use function fwrite;
use function gettype;
use function is_resource;
use function link;
use function rewind;
use function stream_copy_to_stream;
use function stream_get_meta_data;
use function strlen;
use function unlink;

use const SEEK_END;

final class NativeLocalDestinationStream implements DestinationStream
{
    /**
     * @var null|closed-resource|resource
     */
    private $handle;

    private ?Path $target = null;

    /**
     * @param Path $path
     * @param resource $handle
     */
    public function __construct(
        private readonly Path $path,
        $handle,
    ) {
        if (!is_resource($handle)) {
            throw new InvalidArgumentException('DestinationStream expects resource type, given: ' . gettype($handle));
        }

        $this->handle = $handle;
    }

    public static function openAppend(Path $path): self
    {
        $resource = fopen($path->path(), 'ab', false, $path->context()->resource());

        if ($resource === false) {
            throw new RuntimeException("Cannot open file: {$path->uri()}");
        }

        return new self($path, $resource);
    }

    public static function openBlank(Path $path): self
    {
        $resource = fopen($path->path(), 'wb', false, $path->context()->resource());

        if ($resource === false) {
            throw new RuntimeException("Cannot open file: {$path->uri()}");
        }

        return new self($path, $resource);
    }

    public static function openExclusive(Path $path, Path $temporary): self
    {
        $stream = self::openBlank($temporary);
        $stream->target = $path;

        return $stream;
    }

    public function append(string $data): self
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot write to closed stream');
        }

        fseek($this->handle(), 0, SEEK_END);

        $written = fwrite($this->handle(), $data);

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
     * @throws FileExistsException when the stream is exclusive and the target exists
     */
    public function close(): void
    {
        if (!is_resource($this->handle)) {
            $this->handle = null;

            return;
        }

        fclose($this->handle());
        $this->handle = null;

        if ($this->target === null) {
            return;
        }

        $linked = @link($this->path->path(), $this->target->path());
        $error = error_get_last()['message'] ?? '';

        unlink($this->path->path());

        if ($linked) {
            return;
        }

        if (file_exists($this->target->path())) {
            throw new FileExistsException('File already exists: ' . $this->target->uri());
        }

        throw new RuntimeException("File {$this->target->uri()} was not created: {$error}");
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource): self
    {
        if (!is_resource($resource)) {
            throw new InvalidArgumentException(
                'DestinationStream::fromResource expects resource type, given: ' . gettype($resource),
            );
        }

        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot write to closed stream');
        }

        $meta = stream_get_meta_data($resource);

        if ($meta['seekable']) {
            rewind($resource);
        }

        stream_copy_to_stream($resource, $this->handle(), null);

        return $this;
    }

    public function isOpen(): bool
    {
        return is_resource($this->handle);
    }

    public function path(): Path
    {
        return $this->target ?? $this->path;
    }

    /**
     * @return resource
     */
    private function handle()
    {
        if (!$this->isOpen() || $this->handle === null) {
            throw new RuntimeException('Cannot read from closed stream');
        }

        return $this->handle;
    }
}

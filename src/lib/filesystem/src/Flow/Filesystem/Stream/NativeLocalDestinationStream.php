<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\{DestinationStream,
    Exception\InvalidArgumentException,
    Exception\RuntimeException,
    Path
};

final class NativeLocalDestinationStream implements DestinationStream
{
    /**
     * @var null|closed-resource|resource
     */
    private $handle;

    /**
     * @param Path $path
     * @param resource $handle
     */
    public function __construct(private readonly Path $path, $handle)
    {
        if (!\is_resource($handle)) {
            throw new InvalidArgumentException('DestinationStream expects resource type, given: ' . \gettype($handle));
        }

        $this->handle = $handle;
    }

    public static function openAppend(Path $path) : self
    {
        $resource = \fopen($path->path(), 'ab', false, $path->context()->resource());

        if ($resource === false) {
            throw new RuntimeException("Cannot open file: {$path->uri()}");
        }

        return new self($path, $resource);
    }

    public static function openBlank(Path $path) : self
    {
        $resource = \fopen($path->path(), 'wb', false, $path->context()->resource());

        if ($resource === false) {
            throw new RuntimeException("Cannot open file: {$path->uri()}");
        }

        return new self($path, $resource);
    }

    public function append(string $data) : self
    {
        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot write to closed stream');
        }

        \fseek($this->handle, 0, \SEEK_END);
        \fwrite($this->handle, $data);

        return $this;
    }

    public function close() : void
    {
        if (!\is_resource($this->handle)) {
            $this->handle = null;

            return;
        }

        \fclose($this->handle);
        $this->handle = null;
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource) : self
    {
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('DestinationStream::fromResource expects resource type, given: ' . \gettype($resource));
        }

        if (!$this->isOpen()) {
            throw new RuntimeException('Cannot write to closed stream');
        }

        $meta = \stream_get_meta_data($resource);

        if ($meta['seekable']) {
            \rewind($resource);
        }

        \stream_copy_to_stream($resource, $this->handle);

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

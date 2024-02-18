<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem\Stream;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem\Path;

final class FileStream
{
    /**
     * @param Path $path
     * @param null|resource $resource
     */
    public function __construct(private readonly Path $path, private $resource)
    {
        if (!\is_resource($this->resource)) {
            throw new InvalidArgumentException('FileStream expects resource type, given: ' . \gettype($this->resource));
        }
    }

    public static function voidStream(Path $path) : self
    {
        VoidStreamWrapper::register();

        $resourcePath = 'void://' . $path->uri();

        $resource = \fopen($resourcePath, Mode::WRITE->value);

        if ($resource === false) {
            throw new RuntimeException("Cannot open void stream for {$resourcePath}");
        }

        return new self($path, $resource);
    }

    /**
     * @psalm-suppress InvalidPropertyAssignmentValue
     */
    public function close() : void
    {
        if (!\is_resource($this->resource)) {
            throw new RuntimeException('FileStream was closed');
        }

        \fclose($this->resource);
        $this->resource = null;
    }

    public function isOpen() : bool
    {
        return \is_resource($this->resource);
    }

    public function path() : Path
    {
        return $this->path;
    }

    /**
     * @return resource
     */
    public function resource()
    {
        if (!\is_resource($this->resource)) {
            throw new RuntimeException('FileStream was closed');
        }

        return $this->resource;
    }
}

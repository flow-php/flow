<?php

declare(strict_types=1);

namespace Flow\Parquet\Stream;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Stream;

final class LocalStream implements Stream
{
    public function __construct(private $resource)
    {
        if (!\is_resource($this->resource)) {
            throw new InvalidArgumentException('Provided value is not a valid resource, got: ' . \gettype($this->resource) . ' instead of resource');
        }
    }

    public function close() : void
    {
        if ($this->isOpen()) {
            \fclose($this->resource);
        }
    }

    public function isOpen() : bool
    {
        return \is_resource($this->resource);
    }

    public function read(int $length, int $offset, int $whence) : string
    {
        if ($this->isOpen()) {
            \fseek($this->resource, $offset, $whence);

            return \fread($this->resource, $length);
        }

        throw new InvalidArgumentException('Stream is not open');
    }
}

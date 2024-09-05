<?php

declare(strict_types=1);

namespace Flow\Filesystem\Stream;

use Flow\Filesystem\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\Path;

class Block
{
    /**
     * @var resource
     */
    private $handle;

    private int $size = 0;

    /**
     * @param string $id
     * @param int $totalSize
     */
    public function __construct(
        private readonly string $id,
        private readonly int $totalSize,
        private readonly Path $path,
    ) {
        if ($this->id === '') {
            throw new InvalidArgumentException('Block id cannot be empty.');
        }

        if ($totalSize < 0) {
            throw new InvalidArgumentException('Block size must be greater than 0, got: ' . $totalSize);
        }

        if (\file_exists($path->path())) {
            throw new InvalidArgumentException('Block file already exists: ' . $path->path());
        }

        $handle = \fopen($path->path(), 'w+b');

        if ($handle === false) {
            throw new RuntimeException('Could not open block file: ' . $path->path());
        }

        $this->handle = $handle;
    }

    /**
     * @throws RuntimeException when we try to append more data than block can handle
     */
    public function append(string $data) : void
    {
        if ($this->spaceLeft() < strlen($data)) {
            throw new RuntimeException('Block is full, space left: ' . $this->spaceLeft() . ' bytes, trying to append: ' . strlen($data) . ' bytes.');
        }

        \fwrite($this->handle, $data);
        $this->size += strlen($data);
    }

    /**
     * @param resource $resource
     */
    public function fromResource($resource, int $offset = 0) : int
    {
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('Block::fromResource expects resource type, given: ' . \gettype($resource));
        }

        if ($offset < 0) {
            throw new InvalidArgumentException('Block::fromResource expects offset to be greater or equal to 0, given: ' . $offset);
        }

        $result = \stream_copy_to_stream($resource, $this->handle, $this->spaceLeft(), $offset);

        if ($result === false) {
            throw new RuntimeException('Could not copy stream to block.');
        }

        $this->size += $result;

        return $result;
    }

    public function id() : string
    {
        return $this->id;
    }

    public function path() : Path
    {
        return $this->path;
    }

    /**
     * Current block size in bytes.
     */
    public function size() : int
    {
        return $this->size;
    }

    public function spaceLeft() : int
    {
        return $this->totalSize - $this->size;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Buffer;

use Flow\Filesystem\Buffer;
use Flow\Filesystem\Exception\RuntimeException;

final class TempBuffer implements Buffer
{
    /**
     * @var resource
     */
    private $handle;

    private int $size = 0;

    public function __construct(int $memoryLimit = 1024 * 1024 * 8)
    {
        $handle = \fopen('php://temp/maxmemory:' . $memoryLimit, 'wb+');

        if ($handle === false) {
            throw new RuntimeException('Failed to open temporary buffer.');
        }

        $this->handle = $handle;
    }

    public function __destruct()
    {
        \fclose($this->handle);
    }

    public function append(string $data) : void
    {
        \fseek($this->handle, 0, \SEEK_END);
        $writtenBytes = \fwrite($this->handle, $data);

        if ($writtenBytes === false) {
            throw new RuntimeException('Failed to write to temporary buffer.');
        }

        $this->size += $writtenBytes;
    }

    public function dump() : string
    {
        \rewind($this->handle);

        $data = \stream_get_contents($this->handle);

        if ($data === false) {
            throw new RuntimeException('Failed to read from temporary buffer.');
        }

        return $data;
    }

    public function size() : int
    {
        return $this->size;
    }
}

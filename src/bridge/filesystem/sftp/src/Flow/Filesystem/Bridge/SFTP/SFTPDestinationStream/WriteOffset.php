<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP\SFTPDestinationStream;

use Flow\Filesystem\Exception\InvalidArgumentException;

final class WriteOffset
{
    public function __construct(
        private int $offset = 0,
    ) {
        if ($this->offset < 0) {
            throw new InvalidArgumentException('Write offset cannot be negative, got: ' . $this->offset);
        }
    }

    public function advance(int $bytes): void
    {
        if ($bytes < 0) {
            throw new InvalidArgumentException('Cannot advance write offset by a negative value, got: ' . $bytes);
        }

        $this->offset += $bytes;
    }

    public function current(): int
    {
        return $this->offset;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Operations;

use Flow\Filesystem\Exception\InvalidArgumentException;

final readonly class OperationOptions
{
    /** @var int<1, max> */
    public int $chunkSize;

    public function __construct(int $chunkSize = 8192)
    {
        if ($chunkSize < 1) {
            throw new InvalidArgumentException("Chunk size must be greater than zero, got {$chunkSize}.");
        }

        $this->chunkSize = $chunkSize;
    }
}

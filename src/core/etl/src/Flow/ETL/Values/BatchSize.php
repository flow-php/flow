<?php

declare(strict_types=1);

namespace Flow\ETL\Values;

use Flow\ETL\Foundation\Exception\InvalidArgumentException;

final readonly class BatchSize
{
    /**
     * @var int<1, max>
     */
    public int $size;

    public function __construct(int $size = 2048)
    {
        if ($size < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0, got ' . $size);
        }

        $this->size = $size;
    }
}

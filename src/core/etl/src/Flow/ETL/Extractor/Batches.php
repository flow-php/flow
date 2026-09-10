<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;

trait Batches
{
    /**
     * @var int<1, max>
     */
    private int $batchSize = 100;

    public function withBatchSize(int $batchSize): static
    {
        if ($batchSize <= 0) {
            throw new InvalidArgumentException('Batch size must be greater than 0, got ' . $batchSize);
        }

        $this->batchSize = $batchSize;

        return $this;
    }

    /**
     * @return int<1, max>
     */
    public function batchSize(): int
    {
        return $this->batchSize;
    }
}

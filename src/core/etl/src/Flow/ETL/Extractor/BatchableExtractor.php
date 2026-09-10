<?php

declare(strict_types=1);

namespace Flow\ETL\Extractor;

use Flow\ETL\Exception\InvalidArgumentException;

/**
 * A source that decides how many rows it puts in one Rows. withBatchSize() bounds what the source
 * BUILDS; batches() re-slices what a source already emitted and cannot lower its peak.
 */
interface BatchableExtractor
{
    /**
     * Mutates and returns the same instance, so one extractor shared across from_all() children
     * shares its size.
     *
     * @throws InvalidArgumentException when $batchSize is not greater than 0
     */
    public function withBatchSize(int $batchSize): static;

    /**
     * @return int<1, max>
     */
    public function batchSize(): int;
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Transformation;

use Flow\ETL\{DataFrame, Transformation};
use Flow\Filesystem\Exception\InvalidArgumentException;

/**
 * Sets batch size for DataFrame.
 * Small batch size can be useful when processing large data sets since only one row is processed at a time.
 * This means that while processing large data sets, memory usage is kept low.
 *
 * Normally flow allows to use batch size -1 (which means no batches) but it defeats the purpose of using this transformation on
 * Data Streams.
 */
final readonly class BatchSize implements Transformation
{
    /**
     * @param int<1, max> $size
     */
    public function __construct(private int $size)
    {
        if ($size < 1) {
            throw new InvalidArgumentException('Batch size must be greater than 0');
        }
    }

    public function transform(DataFrame $dataFrame) : DataFrame
    {
        return $dataFrame->batchSize($this->size);
    }
}

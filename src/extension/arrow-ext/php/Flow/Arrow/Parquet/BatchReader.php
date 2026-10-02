<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Arrow\ArrowBatch;
use Flow\Arrow\ArrowSchema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class BatchReader
{
    /**
     * @param list<string> $columns root column names
     * @param int<1, max> $batchSize
     */
    public function __construct(ParquetFile $file, array $columns, int $batchSize, ?int $offset, ?int $limit)
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * At most $batchSize rows as one Arrow C Data struct array, null after the last.
     */
    public function next(): ?ArrowBatch
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * The struct schema every batch has, before any batch is read.
     */
    public function schema(): ArrowSchema
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}

<?php

declare(strict_types=1);

namespace Flow\Arrow\Parquet;

use Flow\Arrow\RustArrowSchema;
use Flow\Arrow\RustParquetBatch;
use Flow\Parquet\Engine\RustParquetFileReader;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('arrow')) {
    return;
}

final class RustBatchReader
{
    /**
     * @param list<string> $columns root column names
     * @param int<1, max> $batchSize
     */
    public function __construct(RustParquetFileReader $file, array $columns, int $batchSize, ?int $offset, ?int $limit)
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
    public function next(): ?RustParquetBatch
    {
        throw new RuntimeException('arrow extension is not loaded');
    }

    /**
     * The struct schema every batch has, before any batch is read.
     */
    public function schema(): RustArrowSchema
    {
        throw new RuntimeException('arrow extension is not loaded');
    }
}

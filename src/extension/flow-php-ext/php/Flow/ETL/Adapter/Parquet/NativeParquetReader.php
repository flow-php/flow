<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\Arrow\Parquet\BatchReader;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeParquetReader
{
    /**
     * @param BatchReader $batches arrow-ext's batches of the schema's columns
     * @param Schema $schema the columns to read; every definition's type must store what the file's column reads as,
     *                       checked here, before any batch
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException for a column whose type stores something else
     */
    public function __construct(BatchReader $batches, Schema $schema)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * The next batch under the schema, null after the last.
     */
    public function next(): ?Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}

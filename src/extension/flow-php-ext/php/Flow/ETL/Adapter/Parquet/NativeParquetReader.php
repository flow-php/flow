<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Parquet\Engine\Native\NativeParquetFile;
use RuntimeException;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

final class NativeParquetReader
{
    /**
     * @param NativeParquetFile $file the open file, whose footer plans the read
     * @param Schema $schema the columns to read, in file order or a projection; every definition's type must store
     *                       what the file's column reads as
     * @param int<1, max> $batchSize
     *
     * @throws \Flow\ETL\Exception\InvalidArgumentException for a negative $offset or $limit
     */
    public function __construct(NativeParquetFile $file, Schema $schema, int $batchSize, ?int $offset, ?int $limit)
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    public function close(): void
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }

    /**
     * The next batch of at most $batchSize rows under the schema, null after the last.
     */
    public function next(): ?Rows
    {
        throw new RuntimeException('flow_php extension is not loaded');
    }
}

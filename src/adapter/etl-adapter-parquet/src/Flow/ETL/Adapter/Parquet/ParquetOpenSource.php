<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;

interface ParquetOpenSource
{
    /**
     * Batches of at most $batchSize rows under $schema, every column adopted by $backend.
     *
     * @param int<1, max> $batchSize
     *
     * @return Generator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, ?int $offset, ?int $limit, Backend $backend): Generator;

    public function close(): void;
}

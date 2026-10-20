<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Parquet;

use Flow\ETL\Column\Backend;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Parquet\Reader\ReadPlan;
use Iterator;

use function interface_exists;

if (interface_exists(ParquetOpenSource::class, false)) {
    return;
}

interface ParquetOpenSource
{
    /**
     * Batches of at most $batchSize rows under $schema, every column adopted by $backend.
     *
     * @param int $batchSize refused below 1
     *
     * @return Iterator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, ReadPlan $plan, Backend $backend): Iterator;

    public function close(): void;
}

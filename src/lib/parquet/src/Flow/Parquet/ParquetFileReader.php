<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\ParquetFile\Footer;
use Flow\Parquet\Reader\ReadPlan;
use Iterator;

use function interface_exists;

if (interface_exists(ParquetFileReader::class, false)) {
    return;
}

interface ParquetFileReader
{
    /**
     * Closes the stream it was opened on; any later call throws RuntimeException('Reader is not open').
     */
    public function close(): void;

    /**
     * The file's footer, read on the first call; it stays readable after close().
     */
    public function footer(): Footer;

    /**
     * @param list<string> $columns resolved names, never empty
     * @param int $batchSize upper bound of rows per chunk, refused below 1
     *
     * @return Iterator<int, array<string, list<mixed>>> chunks keyed by column, every list the same length (>0): the rows
     *         $plan selects that match $plan->rowFilter, at most $plan->limit
     */
    public function readColumns(array $columns, int $batchSize, ReadPlan $plan): Iterator;
}

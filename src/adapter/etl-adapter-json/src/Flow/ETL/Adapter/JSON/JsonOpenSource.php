<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON;

use Flow\ETL\Column\Backend;
use Flow\ETL\Exception\SchemaMismatchException;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Iterator;

use function extension_loaded;

if (extension_loaded('flow_php')) {
    return;
}

interface JsonOpenSource
{
    public function close(): void;

    /**
     * Batches of exactly $batchSize rows (the last may be shorter), keyed and ordered by $schema; row indexes in
     * refusals are relative to the batch. This instance is consumed afterwards.
     *
     * @param int<1, max> $batchSize
     *
     * @throws SchemaMismatchException
     *
     * @return Iterator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend): Iterator;
}

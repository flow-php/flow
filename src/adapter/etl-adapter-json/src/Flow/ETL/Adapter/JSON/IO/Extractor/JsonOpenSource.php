<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Schema;
use Iterator;

use function interface_exists;

if (interface_exists(JsonOpenSource::class, false)) {
    return;
}

interface JsonOpenSource
{
    public function close(): void;

    /**
     * Batches of exactly $batchSize rows (the last may be shorter), keyed and ordered by $schema; row indexes in
     * refusals are relative to the source. This instance is consumed afterwards.
     *
     * @param int<1, max> $batchSize
     *
     * @throws SchemaMismatchException
     *
     * @return Iterator<int, Rows>
     */
    public function batches(Schema $schema, int $batchSize, Backend $backend): Iterator;
}

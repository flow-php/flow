<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\JSON\IO\Extractor;

use Flow\ETL\Foundation\Exception\CorruptedRecordException;
use Flow\ETL\Foundation\Exception\InferredSchemaException;
use Flow\ETL\Foundation\Exception\SchemaMismatchException;
use Flow\ETL\IO\Inference\SchemaInference;
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
     * Batches of exactly $batchSize rows (the last may be shorter), keyed and ordered by $schema, and every corrupted
     * record handed up in record order. This instance is consumed afterwards.
     *
     * @param int<1, max> $batchSize
     * @param null|SchemaInference $inference the inference that made $schema; null when it was declared
     * @param null|Schema $known the schema the user sees, $schema plus the file's own columns
     *
     * @throws InferredSchemaException|SchemaMismatchException a refused record under an inferred schema
     *
     * @return Iterator<int, CorruptedRecordException|Rows>
     */
    public function batches(
        Schema $schema,
        int $batchSize,
        Backend $backend,
        ?SchemaInference $inference = null,
        ?Schema $known = null,
    ): Iterator;
}

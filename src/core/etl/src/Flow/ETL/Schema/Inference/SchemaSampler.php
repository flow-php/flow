<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Row\RawRowValues;

interface SchemaSampler
{
    /**
     * One inner iterable per sampling unit (a file, a byte-range chunk, a sheet range), in listing order.
     * The outer iterable is exactly what SchemaInferrer::infer() consumes; a future parallel driver fans
     * SchemaInferrer::sniff() over the inner iterables and merges the ColumnTypes partials.
     * The unit itself is never named as a type - the implementer decides what one inner iterable is.
     *
     * @return iterable<int, iterable<int, RawRowValues>>
     */
    public function samples(int $rowBudget): iterable;
}

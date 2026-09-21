<?php

declare(strict_types=1);

namespace Flow\ETL\Schema\Inference;

use Flow\ETL\Row\RawRowValues;

interface SchemaSampler
{
    /**
     * One inner iterable per sampling unit (a file, a byte-range chunk, a sheet range), in listing order.
     * The outer iterable is exactly what SchemaInferrer::infer() consumes; a unit that implements
     * SniffsColumnTypes folds itself into its partial instead of being iterated row by row.
     *
     * @return iterable<int, iterable<int, RawRowValues>>
     */
    public function samples(int $rowBudget): iterable;
}

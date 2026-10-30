<?php

declare(strict_types=1);

namespace Flow\ETL\Operators\GroupBy;

use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\Reference\Reference;
use Flow\ETL\Values\Rows;

interface PivotValues
{
    /**
     * @param iterable<Rows> $batches read only by values discovered from the rows
     *
     * @throws SchemaNotDerivableException
     */
    public function resolve(iterable $batches, Reference $pivot): DeclaredPivotValues;
}

<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy;

use Flow\ETL\DataFrame;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\Row\Reference;

interface PivotValues
{
    /**
     * @throws SchemaNotDerivableException
     */
    public function resolve(DataFrame $source, Reference $pivot): DeclaredPivotValues;
}

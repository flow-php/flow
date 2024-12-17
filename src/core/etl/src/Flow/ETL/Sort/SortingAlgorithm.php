<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use Flow\ETL\Row\References;
use Flow\ETL\{Extractor, FlowContext, Pipeline};

/**
 * @internal
 */
interface SortingAlgorithm
{
    public function sortBy(Pipeline $pipeline, FlowContext $context, References $refs) : Extractor;
}

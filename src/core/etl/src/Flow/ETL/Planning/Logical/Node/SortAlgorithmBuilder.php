<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Kernels\Sort\ExternalSortConfig;
use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Values\Column\Backend;
use Flow\Filesystem\Path;

interface SortAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): MemorySortConfig|ExternalSortConfig;
}

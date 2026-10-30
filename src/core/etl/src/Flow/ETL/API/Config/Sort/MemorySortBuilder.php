<?php

declare(strict_types=1);

namespace Flow\ETL\API\Config\Sort;

use Flow\ETL\Kernels\Sort\MemorySortConfig;
use Flow\ETL\Planning\Logical\Node\SortAlgorithmBuilder;
use Flow\ETL\Values\Column\Backend;
use Flow\Filesystem\Path;

final class MemorySortBuilder implements SortAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): MemorySortConfig
    {
        return new MemorySortConfig();
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\ETL\Column\Backend;
use Flow\Filesystem\Path;

final class MemorySortBuilder implements SortAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): MemorySortConfig
    {
        return new MemorySortConfig();
    }
}

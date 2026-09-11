<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Sort;

use Flow\Filesystem\Path;

final class MemorySortBuilder implements SortAlgorithmBuilder
{
    public function build(Path $spillRoot): MemorySortConfig
    {
        return new MemorySortConfig();
    }
}

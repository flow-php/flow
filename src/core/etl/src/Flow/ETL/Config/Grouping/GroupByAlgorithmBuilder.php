<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\ETL\Column\Backend;
use Flow\Filesystem\Path;

interface GroupByAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): HashGroupByConfig;
}

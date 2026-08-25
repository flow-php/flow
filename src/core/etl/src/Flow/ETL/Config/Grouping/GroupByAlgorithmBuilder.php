<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Grouping;

use Flow\Filesystem\Path;

interface GroupByAlgorithmBuilder
{
    public function build(Path $spillRoot): HashGroupByConfig;
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Repartition;

use Flow\ETL\Column\Backend;
use Flow\Filesystem\Path;

interface RepartitionAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): HashRepartitionConfig;
}

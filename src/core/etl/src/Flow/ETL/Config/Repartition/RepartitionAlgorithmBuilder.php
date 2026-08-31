<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Repartition;

use Flow\Filesystem\Path;

interface RepartitionAlgorithmBuilder
{
    public function build(Path $spillRoot): HashRepartitionConfig;
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Join;

use Flow\Filesystem\Path;

interface JoinAlgorithmBuilder
{
    public function build(Path $spillRoot): HashJoinConfig;
}

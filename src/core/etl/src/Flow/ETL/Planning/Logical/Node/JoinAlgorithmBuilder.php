<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Operators\Join\HashJoinConfig;
use Flow\ETL\Values\Column\Backend;
use Flow\Filesystem\Path;

interface JoinAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): HashJoinConfig;
}

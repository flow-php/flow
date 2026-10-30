<?php

declare(strict_types=1);

namespace Flow\ETL\Planning\Logical\Node;

use Flow\ETL\Storage\BucketingConfig;
use Flow\ETL\Values\Column\Backend;
use Flow\Filesystem\Path;

interface BucketingAlgorithmBuilder
{
    public function build(Path $spillRoot, Backend $backend): BucketingConfig;
}

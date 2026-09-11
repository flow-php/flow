<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Filesystem\SaveMode;
use Flow\Filesystem\Path;

interface FileLoader
{
    public function destination(): Path;

    public function saveMode(SaveMode $mode): static;
}

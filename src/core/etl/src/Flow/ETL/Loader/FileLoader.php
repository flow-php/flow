<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\Filesystem\Path;

interface FileLoader
{
    public function destination() : Path;
}

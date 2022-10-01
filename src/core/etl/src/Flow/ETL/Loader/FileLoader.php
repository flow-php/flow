<?php

declare(strict_types=1);

namespace Flow\ETL\Loader;

use Flow\ETL\Filesystem\Path;

interface FileLoader
{
    public function destination() : Path;
}

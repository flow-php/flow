<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Filesystem;

use Flow\ETL\Filesystem\Path;

function remote_files(Path $directory, bool $recursive = false) : RemoteFileListExtractor
{
    return new RemoteFileListExtractor($directory, $recursive);
}

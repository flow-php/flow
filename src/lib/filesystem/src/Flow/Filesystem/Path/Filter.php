<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path;

use Flow\Filesystem\FileStatus;

interface Filter
{
    public function accept(FileStatus $status) : bool;
}

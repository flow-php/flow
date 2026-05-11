<?php

declare(strict_types=1);

namespace Flow\Filesystem\Path\Filter;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;

final class KeepAll implements Filter
{
    public function accept(FileStatus $status): bool
    {
        return true;
    }
}

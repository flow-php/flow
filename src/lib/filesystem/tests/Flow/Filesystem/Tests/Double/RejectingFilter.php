<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Path\Filter;

final class RejectingFilter implements Filter
{
    public function accept(FileStatus $status) : bool
    {
        return false;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests;

use const PHP_OS_FAMILY;

trait OperatingSystem
{
    protected function isUnix(): bool
    {
        return PHP_OS_FAMILY !== 'Windows';
    }

    protected function isWindows(): bool
    {
        return PHP_OS_FAMILY === 'Windows';
    }
}

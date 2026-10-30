<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Context;

use const PHP_OS_FAMILY;

final class OperatingSystem
{
    public static function isUnix(): bool
    {
        return PHP_OS_FAMILY !== 'Windows';
    }

    public static function isWindows(): bool
    {
        return PHP_OS_FAMILY === 'Windows';
    }
}

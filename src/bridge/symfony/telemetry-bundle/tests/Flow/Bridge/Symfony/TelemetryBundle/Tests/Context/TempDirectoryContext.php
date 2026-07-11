<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Context;

use FilesystemIterator;
use RecursiveDirectoryIterator;
use RecursiveIteratorIterator;

use function bin2hex;
use function chmod;
use function is_dir;
use function mkdir;
use function random_bytes;
use function rmdir;
use function sys_get_temp_dir;
use function unlink;

final class TempDirectoryContext
{
    public static function remove(string $directory): void
    {
        if (!is_dir($directory)) {
            return;
        }

        // The read-only degradation tests leave directories without write permission behind.
        chmod($directory, 0o755);

        /** @var iterable<\SplFileInfo> $files */
        $files = new RecursiveIteratorIterator(
            new RecursiveDirectoryIterator($directory, FilesystemIterator::SKIP_DOTS),
            RecursiveIteratorIterator::CHILD_FIRST,
        );

        foreach ($files as $file) {
            if ($file->isDir()) {
                chmod($file->getPathname(), 0o755);
                rmdir($file->getPathname());
            } else {
                unlink($file->getPathname());
            }
        }

        rmdir($directory);
    }

    /**
     * @param callable(string): void $test
     */
    public static function with(callable $test): void
    {
        $directory = sys_get_temp_dir() . '/flow_telemetry_bundle_test_' . bin2hex(random_bytes(8));
        mkdir($directory, 0o777, true);

        try {
            $test($directory);
        } finally {
            self::remove($directory);
        }
    }
}

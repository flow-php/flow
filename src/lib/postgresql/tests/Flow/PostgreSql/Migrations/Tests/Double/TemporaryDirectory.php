<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Double;

final class TemporaryDirectory
{
    public readonly string $path;

    public function __construct()
    {
        $this->path = \sys_get_temp_dir() . '/flow_migrations_test_' . \uniqid();
        \mkdir($this->path, 0755, true);
    }

    public function cleanUp() : void
    {
        if (!\is_dir($this->path)) {
            return;
        }

        $iterator = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator($this->path, \RecursiveDirectoryIterator::SKIP_DOTS),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );

        /** @var \SplFileInfo $file */
        foreach ($iterator as $file) {
            if ($file->isDir()) {
                \rmdir($file->getPathname());
            } else {
                \unlink($file->getPathname());
            }
        }

        \rmdir($this->path);
    }
}

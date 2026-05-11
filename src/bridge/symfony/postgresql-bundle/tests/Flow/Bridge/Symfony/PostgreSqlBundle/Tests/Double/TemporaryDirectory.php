<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Double;

use Flow\Filesystem\Local\NativeLocalFilesystem;

use function Flow\Filesystem\DSL\path_real;

final readonly class TemporaryDirectory
{
    public string $path;

    public function __construct()
    {
        $fs = new NativeLocalFilesystem();
        $this->path = path_real($fs->getSystemTmpDir()->path() . '/flow_migrations_test_' . \uniqid())->path();
        \mkdir($this->path, 0755, true);
    }

    public function cleanUp(): void
    {
        (new NativeLocalFilesystem())->rm(path_real($this->path));
    }
}

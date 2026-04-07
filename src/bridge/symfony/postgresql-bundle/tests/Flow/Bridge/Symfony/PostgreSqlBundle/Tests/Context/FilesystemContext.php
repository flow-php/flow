<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context;

use function Flow\Filesystem\DSL\{native_local_filesystem, path};
use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

final readonly class FilesystemContext
{
    private NativeLocalFilesystem $filesystem;

    private Path $workDir;

    public function __construct(string $prefix = 'flow_test_')
    {
        $this->filesystem = native_local_filesystem();
        $this->workDir = path($this->filesystem->getSystemTmpDir()->path() . '/' . $prefix . \bin2hex(\random_bytes(4)));
        $this->filesystem->writeTo(path($this->workDir->path() . '/.keep'))->append('')->close();
    }

    public function cleanup() : void
    {
        $this->filesystem->rm($this->workDir);
    }

    public function filesystem() : NativeLocalFilesystem
    {
        return $this->filesystem;
    }

    public function path(string $relative = '') : Path
    {
        if ($relative === '') {
            return $this->workDir;
        }

        return path($this->workDir->path() . '/' . \ltrim($relative, '/'));
    }

    public function readFile(string $relative) : string
    {
        return $this->filesystem->readFrom($this->path($relative))->content();
    }

    public function writeFile(string $relative, string $content) : Path
    {
        $path = $this->path($relative);
        $this->filesystem->writeTo($path)->append($content)->close();

        return $path;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Context;

use Flow\Filesystem\Local\NativeLocalFilesystem;
use Flow\Filesystem\Path;

use function bin2hex;
use function dirname;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function ltrim;
use function random_bytes;

final readonly class FilesystemContext
{
    private NativeLocalFilesystem $filesystem;

    private Path $workDir;

    public function __construct(string $prefix = 'flow_test_')
    {
        $this->filesystem = native_local_filesystem();
        $this->workDir = path(dirname(__DIR__, 7) . '/var/tests/' . $prefix . bin2hex(random_bytes(4)));
        $this->filesystem
            ->writeTo(path($this->workDir->path() . '/.keep'))
            ->append('')
            ->close();
    }

    public function cleanup(): void
    {
        $this->filesystem->rm($this->workDir);
    }

    public function filesystem(): NativeLocalFilesystem
    {
        return $this->filesystem;
    }

    public function path(string $relative = ''): Path
    {
        if ($relative === '') {
            return $this->workDir;
        }

        return path($this->workDir->path() . '/' . ltrim($relative, '/'));
    }

    public function readFile(string $relative): string
    {
        return $this->filesystem->readFrom($this->path($relative))->content();
    }

    public function writeFile(string $relative, string $content): Path
    {
        $path = $this->path($relative);
        $this->filesystem->writeTo($path)->append($content)->close();

        return $path;
    }
}

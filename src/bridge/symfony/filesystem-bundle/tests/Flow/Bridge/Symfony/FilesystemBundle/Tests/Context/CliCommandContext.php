<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Context;

use function Flow\Filesystem\DSL\path;
use Flow\Bridge\Symfony\FilesystemBundle\Command\FstabResolver;
use Flow\Filesystem\FilesystemTable;
use Flow\Filesystem\Local\{MemoryFilesystem, NativeLocalFilesystem};
use Symfony\Component\DependencyInjection\ServiceLocator;

final class CliCommandContext
{
    /** @var list<string> */
    private array $tempPaths = [];

    public function cleanup() : void
    {
        foreach ($this->tempPaths as $path) {
            if (\is_file($path)) {
                @\unlink($path);
            } elseif (\is_dir($path)) {
                $this->rmDirRecursive($path);
            }
        }

        $this->tempPaths = [];
    }

    public function defaultTable() : FilesystemTable
    {
        $table = new FilesystemTable();
        $table->mount(new MemoryFilesystem());
        $table->mount(new NativeLocalFilesystem());

        return $table;
    }

    public function resolver(?FilesystemTable $default = null, ?FilesystemTable $secondary = null) : FstabResolver
    {
        $services = [
            'default' => $default ?? $this->defaultTable(),
        ];

        if ($secondary !== null) {
            $services['secondary'] = $secondary;
        }

        $factories = [];

        foreach ($services as $name => $table) {
            $captured = $table;
            $factories[$name] = static fn () : FilesystemTable => $captured;
        }

        return new FstabResolver(new ServiceLocator($factories), 'default');
    }

    public function secondaryMemoryOnly() : FilesystemTable
    {
        $table = new FilesystemTable();
        $table->mount(new MemoryFilesystem());

        return $table;
    }

    public function seedMemoryFile(FilesystemTable $table, string $uri, string $content) : void
    {
        $path = path($uri);
        $stream = $table->for($path)->writeTo($path);
        $stream->append($content);
        $stream->close();
    }

    public function tempDir() : string
    {
        $dir = \sys_get_temp_dir() . '/flow_filesystem_cli_' . \bin2hex(\random_bytes(6));
        \mkdir($dir, 0o777, true);
        $this->tempPaths[] = $dir;

        return $dir;
    }

    private function rmDirRecursive(string $dir) : void
    {
        foreach (\scandir($dir) ?: [] as $entry) {
            if ($entry === '.' || $entry === '..') {
                continue;
            }

            $path = $dir . '/' . $entry;

            if (\is_dir($path)) {
                $this->rmDirRecursive($path);
            } else {
                @\unlink($path);
            }
        }

        @\rmdir($dir);
    }
}

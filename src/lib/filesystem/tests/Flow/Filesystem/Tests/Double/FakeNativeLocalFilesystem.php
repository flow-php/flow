<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Local\GlobWalker;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Generator;

use function array_reverse;
use function file_exists;
use function Flow\Filesystem\DSL\path;
use function Flow\Filesystem\DSL\path_real;
use function in_array;
use function is_dir;
use function is_file;
use function mkdir;
use function rename;
use function rmdir;
use function scandir;
use function sprintf;
use function str_ends_with;
use function sys_get_temp_dir;
use function unlink;

final class FakeNativeLocalFilesystem implements Filesystem
{
    private readonly Mount $mount;

    public function __construct(string $protocol = 'fake')
    {
        $this->mount = new Mount($protocol);
    }

    public function appendTo(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (
                !mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true)
                && !is_dir($concurrentDirectory)
            ) {
                throw new RuntimeException(sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalDestinationStream::openAppend($path);
    }

    public function getSystemTmpDir(): Path
    {
        return path(sys_get_temp_dir());
    }

    public function list(Path $path, Filter $pathFilter = new OnlyFiles()): Generator
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if ($pathFilter->accept($status = new FileStatus($path, is_file($path->path())))) {
                yield $status;
            }

            return;
        }

        foreach ((new GlobWalker())->walk($path) as $filePath) {
            $status = new FileStatus(path_real($filePath, $path->options()), is_file($filePath));

            if ($pathFilter->accept($status)) {
                yield $status;
            }
        }
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        if (!$this->mount->supports($from)) {
            throw new InvalidSchemeException($from->protocol(), $this->mount->protocol);
        }
        if (!$this->mount->supports($to)) {
            throw new InvalidSchemeException($to->protocol(), $this->mount->protocol);
        }

        if (file_exists($to->path())) {
            $this->rm($to);
        }

        if (!rename($from->path(), $to->path())) {
            return false;
        }

        return true;
    }

    public function readFrom(Path $path): SourceStream
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be open: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (
                !mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true)
                && !is_dir($concurrentDirectory)
            ) {
                throw new RuntimeException(sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalSourceStream::open($path);
    }

    public function rm(Path $path): bool
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern()) {
            if (!file_exists($path->path())) {
                return false;
            }

            if (is_dir($path->path())) {
                $this->rmdir($path->path());
            } else {
                unlink($path->path());
            }

            return true;
        }

        $deletedCount = 0;

        foreach (array_reverse((new GlobWalker())->walk($path)) as $filePath) {
            if (is_dir($filePath)) {
                $this->rmdir($filePath);
            } else {
                unlink($filePath);
            }

            $deletedCount++;
        }

        return (bool) $deletedCount;
    }

    public function status(Path $path): ?FileStatus
    {
        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if (!$path->isPattern() && file_exists($path->path())) {
            return new FileStatus($path, is_file($path->path()));
        }

        $filePath = (new GlobWalker())->walk($path)[0] ?? null;

        return $filePath === null ? null : new FileStatus(path($filePath, $path->options()), true);
    }

    public function supports(Path $path): bool
    {
        return $this->mount()->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        if (!$this->mount->supports($path)) {
            throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
        }

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (
                !mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true)
                && !is_dir($concurrentDirectory)
            ) {
                throw new RuntimeException(sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalDestinationStream::openBlank($path);
    }

    private function rmdir(string $dirPath): void
    {
        if (!is_dir($dirPath)) {
            throw new InvalidArgumentException("{$dirPath} must be a directory");
        }

        if (!str_ends_with($dirPath, '/')) {
            $dirPath .= '/';
        }

        $files = scandir($dirPath);

        if (!$files) {
            throw new RuntimeException("Can't read directory: {$dirPath}");
        }

        foreach ($files as $file) {
            if (in_array($file, ['.', '..'], true)) {
                continue;
            }

            $filePath = $dirPath . $file;

            if (is_dir($filePath)) {
                $this->rmdir($filePath);
            } else {
                unlink($filePath);
            }
        }

        rmdir($dirPath);
    }
}

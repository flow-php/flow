<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Double;

use function Flow\Filesystem\DSL\path_real;
use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, Protocol, SourceStream};
use Flow\Filesystem\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\Stream\{NativeLocalDestinationStream, NativeLocalSourceStream};
use Webmozart\Glob\Glob;

final class FakeNativeLocalFilesystem implements Filesystem
{
    #[\Override]
    public function appendTo(Path $path) : DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->protocol()->validateScheme($path);

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (!\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true) && !\is_dir($concurrentDirectory)) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalDestinationStream::openAppend($path);
    }

    #[\Override]
    public function getSystemTmpDir() : Path
    {
        return \Flow\Filesystem\DSL\path(\sys_get_temp_dir());
    }

    #[\Override]
    public function list(Path $path, Filter $pathFilter = new OnlyFiles()) : \Generator
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if ($pathFilter->accept($status = new FileStatus($path, \is_file($path->path())))) {
                yield $status;
            }

            return;

        }

        foreach (Glob::glob($path->path()) as $filePath) {
            $status = new FileStatus(path_real($filePath, $path->options()), \is_file($filePath));

            if ($pathFilter->accept($status)) {
                yield $status;
            }
        }
    }

    #[\Override]
    public function mv(Path $from, Path $to) : bool
    {
        $this->protocol()->validateScheme($from);
        $this->protocol()->validateScheme($to);

        if (\file_exists($to->path())) {
            $this->rm($to);
        }

        if (!\rename($from->path(), $to->path())) {
            return false;
        }

        return true;
    }

    #[\Override]
    public function protocol() : Protocol
    {
        return new Protocol('fake');
    }

    #[\Override]
    public function readFrom(Path $path) : SourceStream
    {
        $this->protocol()->validateScheme($path);

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be open: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (!\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true) && !\is_dir($concurrentDirectory)) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalSourceStream::open($path);
    }

    #[\Override]
    public function rm(Path $path) : bool
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if (!\file_exists($path->path())) {
                return false;
            }

            if (\is_dir($path->path())) {
                $this->rmdir($path->path());
            } else {
                \unlink($path->path());
            }

            return true;
        }

        $deletedCount = 0;

        foreach (Glob::glob($path->path()) as $filePath) {
            if (\is_dir($filePath)) {
                $this->rmdir($filePath);
            } else {
                \unlink($filePath);
            }

            $deletedCount++;
        }

        return (bool) $deletedCount;
    }

    #[\Override]
    public function status(Path $path) : ?FileStatus
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern() && \file_exists($path->path())) {
            return new FileStatus(
                $path,
                \is_file($path->path())
            );
        }

        foreach (Glob::glob($path->path()) as $filePath) {
            if (\file_exists($filePath)) {
                return new FileStatus(\Flow\Filesystem\DSL\path($filePath, $path->options()), true);
            }
        }

        return null;
    }

    #[\Override]
    public function writeTo(Path $path) : DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->protocol()->validateScheme($path);

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (!\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true) && !\is_dir($concurrentDirectory)) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalDestinationStream::openBlank($path);
    }

    private function rmdir(string $dirPath) : void
    {
        if (!\is_dir($dirPath)) {
            throw new InvalidArgumentException("{$dirPath} must be a directory");
        }

        if (!\str_ends_with($dirPath, '/')) {
            $dirPath .= '/';
        }

        $files = \scandir($dirPath);

        if (!$files) {
            throw new RuntimeException("Can't read directory: {$dirPath}");
        }

        foreach ($files as $file) {
            if (\in_array($file, ['.', '..'], true)) {
                continue;
            }

            $filePath = $dirPath . $file;

            if (\is_dir($filePath)) {
                $this->rmdir($filePath);
            } else {
                \unlink($filePath);
            }
        }

        \rmdir($dirPath);
    }
}

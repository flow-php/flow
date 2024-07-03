<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\Exception\{InvalidArgumentException, RuntimeException};
use Flow\Filesystem\Stream\{NativeLocalDestinationStream, NativeLocalSourceStream};
use Flow\Filesystem\{DestinationStream, FileStatus, Filesystem, Path, Path\Filter, Protocol, SourceStream};
use Webmozart\Glob\Glob;

/**
 * This implementation is based on the native PHP filesystem functions documented here: https://www.php.net/manual/en/book.filesystem.php
 * Additionally, in order to support glob pattern `\/**\/` for matching zero or more directories it's using webmozart/glob library.
 */
final class NativeLocalFilesystem implements Filesystem
{
    public function list(Path $path, Filter $pathFilter = new Filter\OnlyFiles()) : \Generator
    {
        $this->protocol()->validateScheme($path);

        if (!$path->isPattern()) {
            if ($pathFilter->accept($status = new FileStatus($path, \is_file($path->path())))) {
                yield $status;
            }

            return;

        }

        foreach (Glob::glob($path->path()) as $filePath) {
            $status = new FileStatus(Path::realpath($filePath, $path->options()), \is_file($filePath));

            if ($pathFilter->accept($status)) {
                yield $status;
            }
        }
    }

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

    public function protocol() : Protocol
    {
        return new Protocol('file');
    }

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
                return new FileStatus(new Path($filePath, $path->options()), true);
            }
        }

        return null;
    }

    public function writeTo(Path $path) : DestinationStream
    {
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

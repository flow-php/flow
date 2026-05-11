<?php

declare(strict_types=1);

namespace Flow\Filesystem\Local;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\OnlyFiles;
use Flow\Filesystem\SourceStream;
use Flow\Filesystem\Stream\NativeLocalDestinationStream;
use Flow\Filesystem\Stream\NativeLocalSourceStream;
use Webmozart\Glob\Glob;
use Webmozart\Glob\Iterator\GlobFilterIterator;
use Webmozart\Glob\Iterator\GlobIterator;

use function Flow\Filesystem\DSL\path_real;
use function Flow\Types\DSL\type_string;

/**
 * This implementation is based on the native PHP filesystem functions documented here: https://www.php.net/manual/en/book.filesystem.php
 * Additionally, in order to support glob pattern `\/**\/` for matching zero or more directories it's using webmozart/glob library.
 */
final readonly class NativeLocalFilesystem implements Filesystem
{
    public function __construct(
        private Mount $mount = new Mount('file'),
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (
                !\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true)
                && !\is_dir($concurrentDirectory)
            ) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalDestinationStream::openAppend($path);
    }

    public function getSystemTmpDir(): Path
    {
        return \Flow\Filesystem\DSL\path(\sys_get_temp_dir());
    }

    public function list(Path $path, Filter $pathFilter = new OnlyFiles()): \Generator
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if (!$path->isPattern()) {
            if ($pathFilter->accept($status = self::statFor($path, $path->path()))) {
                yield $status;
            }

            return;
        }

        foreach (new GlobIterator($path->path()) as $filePath) {
            $filePath = type_string()->assert($filePath);
            $status = self::statFor(path_real($filePath, $path->options()), $filePath);

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
        $this->mount->supports($from) || throw new InvalidSchemeException($from->protocol(), $this->mount->protocol);
        $this->mount->supports($to) || throw new InvalidSchemeException($to->protocol(), $this->mount->protocol);

        if (\file_exists($to->path())) {
            $this->rm($to);
        }

        if (!\rename($from->path(), $to->path())) {
            return false;
        }

        return true;
    }

    public function readFrom(Path $path): SourceStream
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be open: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (
                !\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true)
                && !\is_dir($concurrentDirectory)
            ) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalSourceStream::open($path);
    }

    public function rm(Path $path): bool
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

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

        foreach ($this->matchChildFirst($path->path()) as $filePath) {
            $filePath = type_string()->assert($filePath);

            if (\is_dir($filePath)) {
                $this->rmdir($filePath);
            } else {
                \unlink($filePath);
            }

            $deletedCount++;
        }

        return (bool) $deletedCount;
    }

    public function status(Path $path): ?FileStatus
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if (!$path->isPattern()) {
            if (!\file_exists($path->path())) {
                return null;
            }

            return self::statFor($path, $path->path());
        }

        foreach (new GlobIterator($path->path()) as $filePath) {
            $filePath = type_string()->assert($filePath);

            if (\file_exists($filePath)) {
                return self::statFor(\Flow\Filesystem\DSL\path($filePath, $path->options()), $filePath);
            }
        }

        return null;
    }

    public function writeTo(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be written: " . $path->uri());
        }

        if (!$this->status($path->parentDirectory())) {
            if (
                !\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true)
                && !\is_dir($concurrentDirectory)
            ) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return NativeLocalDestinationStream::openBlank($path);
    }

    /**
     * Lazy iterator over glob matches in CHILD_FIRST order so callers can safely delete each match
     * without confusing webmozart/glob's internal RecursiveIteratorIterator (which descends with SELF_FIRST).
     */
    private function matchChildFirst(string $glob): \Iterator
    {
        $glob = self::canonicalizePath($glob);
        $basePath = Glob::getBasePath($glob);

        if (!\is_dir($basePath)) {
            return new \EmptyIterator();
        }

        $recursive = new \RecursiveIteratorIterator(
            new \RecursiveDirectoryIterator(
                $basePath,
                \RecursiveDirectoryIterator::CURRENT_AS_PATHNAME | \RecursiveDirectoryIterator::SKIP_DOTS,
            ),
            \RecursiveIteratorIterator::CHILD_FIRST,
        );

        return new GlobFilterIterator(
            $glob,
            (static function () use ($recursive) {
                foreach ($recursive as $path) {
                    yield self::canonicalizePath(type_string()->assert($path));
                }
            })(),
            GlobFilterIterator::FILTER_VALUE,
        );
    }

    private function rmdir(string $dirPath): void
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

    private static function canonicalizePath(string $path): string
    {
        return type_string()->cast(\preg_replace('#/+#', '/', \str_replace('\\', '/', $path)));
    }

    private static function statFor(Path $path, string $absolutePath): FileStatus
    {
        $isFile = \is_file($absolutePath);
        $mtime = \filemtime($absolutePath);

        return new FileStatus(
            $path,
            $isFile,
            $isFile ? (\filesize($absolutePath) ?: null) : null,
            $mtime !== false ? new \DateTimeImmutable('@' . $mtime) : null,
        );
    }
}

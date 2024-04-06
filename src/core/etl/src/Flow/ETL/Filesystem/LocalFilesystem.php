<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Flow\ETL\Exception\{FileNotFoundException, InvalidArgumentException, RuntimeException};
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Stream\{FileStream, Mode};
use Flow\ETL\Partition\{NoopFilter, PartitionFilter};
use Webmozart\Glob\Glob;

final class LocalFilesystem implements Filesystem
{
    public function directoryExists(Path $path) : bool
    {
        if (!$path->isLocal()) {
            return false;
        }

        if ($path->isPattern()) {
            return false;
        }

        return \is_dir($path->path());
    }

    public function exists(Path $path) : bool
    {
        if (!$path->isLocal()) {
            return false;
        }

        if (!$path->isPattern()) {
            return \file_exists($path->path());
        }

        foreach (Glob::glob($path->path()) as $filePath) {
            if (\file_exists($filePath)) {
                return true;
            }
        }

        return false;
    }

    public function fileExists(Path $path) : bool
    {
        if (!$path->isLocal()) {
            return false;
        }

        if (!$path->isPattern()) {
            return \file_exists($path->path()) && !\is_dir($path->path());
        }

        foreach (Glob::glob($path->path()) as $filePath) {
            if (\is_dir($filePath)) {
                continue;
            }

            return true;
        }

        return false;
    }

    public function mv(Path $from, Path $to) : void
    {
        if (!$from->isLocal() || !$to->isLocal()) {
            throw new InvalidArgumentException(\sprintf('Paths "%s" and "%s" are not local', $from->uri(), $to->uri()));
        }

        if ($from->isPattern() || $to->isPattern()) {
            throw new InvalidArgumentException('Pattern paths can\'t be moved');
        }

        if (\file_exists($to->path())) {
            $this->rm($to);
        }

        if (!\rename($from->path(), $to->path())) {
            throw new InvalidArgumentException(\sprintf('Can\'t move "%s" to "%s"', $from->uri(), $to->uri()));
        }
    }

    public function open(Path $path, Mode $mode) : FileStream
    {
        if (!$path->isLocal()) {
            throw new InvalidArgumentException(\sprintf('Path "%s" is not local', $path->uri()));
        }

        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be open: " . $path->uri());
        }

        if (!$this->directoryExists($path->parentDirectory())) {
            if (!\mkdir($concurrentDirectory = $path->parentDirectory()->path(), recursive: true) && !\is_dir($concurrentDirectory)) {
                throw new RuntimeException(\sprintf('Directory "%s" was not created', $concurrentDirectory));
            }
        }

        return new FileStream($path, \fopen($path->path(), $mode->value, false, $path->context()->resource()) ?: null);
    }

    public function rm(Path $path) : void
    {
        if (!$path->isLocal()) {
            throw new InvalidArgumentException(\sprintf('Path "%s" is not local', $path->uri()));
        }

        if (!$path->isPattern()) {
            if (\is_dir($path->path())) {
                $this->rmdir($path->path());
            } else {
                if (\file_exists($path->path())) {
                    \unlink($path->path());
                }
            }

            return;
        }

        foreach (Glob::glob($path->path()) as $filePath) {
            if (\is_dir($filePath)) {
                $this->rmdir($filePath);
            } else {
                \unlink($filePath);
            }
        }
    }

    public function scan(Path $path, PartitionFilter $partitionFilter = new NoopFilter()) : \Generator
    {
        if (!$path->isLocal()) {
            throw new InvalidArgumentException(\sprintf('Path "%s" is not local', $path->uri()));
        }

        if (!$path->isPattern()) {
            if (!$this->fileExists($path)) {
                throw new FileNotFoundException($path);
            }

            yield $path;

            return;

        }

        foreach (Glob::glob($path->path()) as $filePath) {
            if (\is_dir($filePath)) {
                continue;
            }

            if ($partitionFilter->keep(...(Path::realpath($filePath, $path->options()))->partitions()->toArray())) {
                yield Path::realpath($filePath, $path->options());
            }
        }
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

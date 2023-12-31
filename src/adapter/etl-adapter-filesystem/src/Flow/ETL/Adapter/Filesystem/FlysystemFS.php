<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Filesystem;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Path;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\Filesystem as Flysystem;
use League\Flysystem\FilesystemException;

final class FlysystemFS implements Filesystem
{
    public function __construct(private readonly FlysystemFactory $factory = new FlysystemFactory())
    {
    }

    public function directoryExists(Path $path) : bool
    {
        $fs = $this->factory->create($path);

        if ($path->isPattern()) {
            return false;
        }

        return $fs->directoryExists($path->path());
    }

    public function exists(Path $path) : bool
    {
        $fs = $this->factory->create($path);

        if ($path->isPattern()) {
            $anyFileExistsInPattern = false;

            foreach ($this->scan($path, new NoopFilter()) as $nextPath) {
                $anyFileExistsInPattern = true;

                break;
            }

            return $anyFileExistsInPattern;
        }

        return $fs->fileExists($path->path()) || $fs->directoryExists($path->path());
    }

    public function fileExists(Path $path) : bool
    {
        $fs = $this->factory->create($path);

        if ($path->isPattern()) {
            $anyFileExistsInPattern = false;

            foreach ($this->scan($path, new NoopFilter()) as $nextPath) {
                $anyFileExistsInPattern = true;

                break;
            }

            return $anyFileExistsInPattern;
        }

        return $fs->fileExists($path->path());
    }

    public function open(Path $path, Mode $mode) : FileStream
    {
        if ($path->isPattern()) {
            throw new InvalidArgumentException("Pattern paths can't be open: " . $path->uri());
        }

        if ($path->isLocal()) {
            $fs = $this->factory->create($path);

            if (!$fs->directoryExists($path->parentDirectory()->path())) {
                $fs->createDirectory($path->parentDirectory()->path());
            }

            return new FileStream($path, \fopen($path->path(), $mode->value, false, $path->context()->resource()) ?: null);
        }

        return new FileStream($path, \fopen($path->uri(), $mode->value, false, $path->context()->resource()) ?: null);
    }

    public function rm(Path $path) : void
    {
        $fs = $this->factory->create($path);

        if ($path->isPattern()) {
            foreach ($this->scan($path, new NoopFilter()) as $nextPath) {
                if ($fs->fileExists($nextPath->path())) {
                    $fs->delete($nextPath->path());
                } else {
                    $fs->deleteDirectory($nextPath->path());
                }
            }

            return;
        }

        if ($fs->fileExists($path->path())) {
            $fs->delete($path->path());

            return;
        }

        if ($fs->directoryExists($path->path())) {
            $fs->deleteDirectory($path->path());

            return;
        }

        throw new RuntimeException("Can't remove path because it does not exists, path: " . $path->uri());
    }

    /**
     * @throws InvalidArgumentException
     * @throws MissingDependencyException
     * @throws FilesystemException
     *
     * @return \Generator<Path>
     */
    public function scan(Path $path, PartitionFilter $partitionFilter = new NoopFilter()) : \Generator
    {
        $fs = $this->factory->create($path);

        if ($fs->fileExists($path->path())) {
            yield $path;

            return;
        }

        $filter = function (FileAttributes|DirectoryAttributes $file) use ($path, $partitionFilter) : bool {
            if ($file instanceof DirectoryAttributes) {
                return false;
            }

            if ($path->isPattern()) {
                if (!$path->matches(new Path($path->scheme() . '://' . $file->path(), $path->options()))) {
                    return false;
                }
            }

            return $partitionFilter->keep(...(new Path(DIRECTORY_SEPARATOR . $file->path()))->partitions()->toArray());
        };

        /**
         * @psalm-suppress ArgumentTypeCoercion
         *
         * @phpstan-ignore-next-line
         */
        foreach ($fs->listContents($path->staticPart()->path(), Flysystem::LIST_DEEP)->filter($filter) as $file) {
            yield new Path($path->scheme() . '://' . $file->path(), $path->options());
        }
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\FileStatus;
use Flow\Filesystem\Filesystem;
use Flow\Filesystem\Mount;
use Flow\Filesystem\Path;
use Flow\Filesystem\Path\Filter;
use Flow\Filesystem\Path\Filter\KeepAll;
use Flow\Filesystem\SourceStream;
use Generator;
use phpseclib4\Exception\BaseException;
use phpseclib4\Exception\FileSystemException;
use phpseclib4\Net\SFTP;

use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function rtrim;

final readonly class SFTPFilesystem implements Filesystem
{
    public function __construct(
        private Mount $mount,
        private SFTP $sftp,
        private Options $options = new Options(),
    ) {}

    public function appendTo(Path $path): DestinationStream
    {
        $this->guardWritable($path);
        $this->createParentDirectory($path);

        return SFTPDestinationStream::openAppend(
            $this->sftp,
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize(),
        );
    }

    public function getSystemTmpDir(): Path
    {
        return $this->options->tmpDir();
    }

    /**
     * @return Generator<int, FileStatus>
     */
    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        $this->guardScheme($path);

        if (!$path->isPattern()) {
            $remotePath = $path->path();

            try {
                if ($this->sftp->is_file($remotePath)) {
                    $fileStatus = new FileStatus(
                        $path,
                        true,
                        type_integer()->assert($this->sftp->filesize($remotePath)),
                        // @mago-expect analysis:less-specific-argument
                        type_datetime()->cast($this->sftp->filemtime($remotePath)),
                    );

                    if ($pathFilter->accept($fileStatus)) {
                        yield $fileStatus;
                    }

                    return;
                }
            } catch (FileSystemException) {
                return;
            }
        }

        $root = rtrim(($path->isPattern() ? $path->staticPart() : $path)->path(), '/') ?: '/';

        foreach ((new DirectoryListing($this->sftp))->read($root) as $entry) {
            $entryPath = path($path->protocol() . '://' . $entry->path, $path->options());

            if ($path->isPattern() && !$path->matches($entryPath)) {
                continue;
            }

            $fileStatus = new FileStatus($entryPath, !$entry->isDirectory, $entry->size, $entry->modifiedAt);

            if ($pathFilter->accept($fileStatus)) {
                yield $fileStatus;
            }
        }
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        $this->guardScheme($from);
        $this->guardScheme($to);

        $remoteFrom = $from->path();
        $remoteTo = $to->path();

        try {
            if (!$this->sftp->file_exists($remoteFrom)) {
                return false;
            }

            $this->createParentDirectory($to);

            $this->sftp->delete($remoteTo, true);
            $this->sftp->rename($remoteFrom, $remoteTo);
        } catch (FileSystemException) {
            return false;
        } catch (BaseException) {
            throw new RuntimeException('SFTP session is no longer usable, cannot move ' . $remoteFrom);
        }

        return true;
    }

    public function readFrom(Path $path): SourceStream
    {
        $this->guardScheme($path);

        return new SFTPSourceStream($path, $this->sftp, $this->options);
    }

    public function rm(Path $path): bool
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return false;
        }

        $this->guardScheme($path);

        $remotePaths = [];

        if ($path->isPattern()) {
            foreach ($this->list($path) as $fileStatus) {
                $remotePaths[] = $fileStatus->path->path();
            }
        } else {
            $remotePaths[] = $path->path();
        }

        $removed = 0;

        foreach ($remotePaths as $remotePath) {
            try {
                if (!$this->sftp->file_exists($remotePath)) {
                    continue;
                }

                $this->sftp->delete($remotePath, true);
                $removed++;
            } catch (FileSystemException) {
                continue;
            } catch (BaseException) {
                throw new RuntimeException('SFTP session is no longer usable, cannot remove ' . $path->path());
            }
        }

        return $removed > 0;
    }

    public function status(Path $path): ?FileStatus
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return new FileStatus($path, false);
        }

        $this->guardScheme($path);

        if (!$path->isPattern()) {
            if ($this->sftp->is_dir($path->path())) {
                return new FileStatus($path, false);
            }
        }

        foreach ($this->list($path) as $fileStatus) {
            return $fileStatus;
        }

        return null;
    }

    public function supports(Path $path): bool
    {
        return $this->mount->supports($path);
    }

    public function writeTo(Path $path): DestinationStream
    {
        $this->guardWritable($path);
        $this->createParentDirectory($path);

        return SFTPDestinationStream::openBlank(
            $this->sftp,
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize(),
        );
    }

    private function createParentDirectory(Path $path): void
    {
        $parent = $path->parentDirectory()->path();

        try {
            if ($this->sftp->is_dir($parent)) {
                return;
            }

            $this->sftp->mkdir($parent, recursive: true);
        } catch (FileSystemException $e) {
            throw new RuntimeException('Could not create directory: ' . $parent, previous: $e);
        }
    }

    private function guardScheme(Path $path): void
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);
    }

    private function guardWritable(Path $path): void
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->guardScheme($path);
    }
}

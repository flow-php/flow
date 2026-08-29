<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\SFTP;

use DateTimeImmutable;
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
use phpseclib3\Net\SFTP;

use function Flow\Types\DSL\type_integer;

final readonly class SFTPFilesystem implements Filesystem
{
    private SFTPSession $session;

    public function __construct(
        private Mount $mount,
        private SFTP $sftp,
        private Options $options = new Options(),
    ) {
        $this->session = new SFTPSession($sftp);
    }

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
            $remotePath = RemotePath::from($path);

            if ($this->sftp->is_file($remotePath->toString())) {
                $fileStatus = $this->fileStatus($path, $remotePath);

                if ($pathFilter->accept($fileStatus)) {
                    yield $fileStatus;
                }

                return;
            }
        }

        yield from (new DirectoryTraversal($this->sftp, $this->session))->walk($path, $pathFilter);
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        $this->guardScheme($from);
        $this->guardScheme($to);

        $remoteFrom = RemotePath::from($from);

        if (!$this->exists($remoteFrom)) {
            $this->session->assertAlive('move ' . $remoteFrom->toString());

            return false;
        }

        $this->createParentDirectory($to);

        $remoteTo = RemotePath::from($to);

        if ($this->exists($remoteTo)) {
            $this->sftp->delete($remoteTo->toString(), true);
        }

        return $this->sftp->rename($remoteFrom->toString(), $remoteTo->toString());
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

        if ($path->isPattern()) {
            return $this->rmMatching($path);
        }

        $remotePath = RemotePath::from($path)->toString();
        $removed = $this->sftp->delete($remotePath, true);

        if (!$removed) {
            $this->session->assertAlive('remove ' . $remotePath);
        }

        return $removed;
    }

    public function status(Path $path): ?FileStatus
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return new FileStatus($path, false);
        }

        $this->guardScheme($path);

        if ($path->isPattern()) {
            foreach ($this->list($path) as $fileStatus) {
                return $fileStatus;
            }

            return null;
        }

        $remotePath = RemotePath::from($path);

        if ($this->sftp->is_dir($remotePath->toString())) {
            return new FileStatus($path, false);
        }

        if (!$this->sftp->is_file($remotePath->toString())) {
            $this->session->assertAlive('stat ' . $remotePath->toString());

            return null;
        }

        return $this->fileStatus($path, $remotePath);
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
        $parent = RemotePath::from($path)->directory();

        if ($parent->isRoot() || $this->sftp->is_dir($parent->toString())) {
            return;
        }

        if (!$this->sftp->mkdir($parent->toString(), -1, true) && !$this->sftp->is_dir($parent->toString())) {
            $this->session->assertAlive('create directory ' . $parent->toString());

            throw new RuntimeException('Could not create directory: ' . $parent->toString());
        }
    }

    private function exists(RemotePath $remotePath): bool
    {
        return $this->sftp->is_file($remotePath->toString()) || $this->sftp->is_dir($remotePath->toString());
    }

    private function fileStatus(Path $path, RemotePath $remotePath): FileStatus
    {
        return new FileStatus(
            $path,
            true,
            (int) $this->sftp->filesize($remotePath->toString()),
            $this->lastModifiedAt($this->sftp->filemtime($remotePath->toString())),
        );
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

    private function lastModifiedAt(mixed $modificationTime): ?DateTimeImmutable
    {
        if (!type_integer()->isValid($modificationTime)) {
            return null;
        }

        return new DateTimeImmutable('@' . $modificationTime);
    }

    private function rmMatching(Path $path): bool
    {
        /** @var array<int, RemotePath> $remotePaths */
        $remotePaths = [];

        foreach ($this->list($path) as $fileStatus) {
            $remotePaths[] = RemotePath::from($fileStatus->path);
        }

        $removed = 0;

        foreach ($remotePaths as $remotePath) {
            if (!$this->exists($remotePath)) {
                continue;
            }

            if ($this->sftp->delete($remotePath->toString(), true)) {
                $removed++;
            }
        }

        return $removed > 0;
    }
}

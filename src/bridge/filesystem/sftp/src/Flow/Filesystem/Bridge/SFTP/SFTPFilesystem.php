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
use SensitiveParameter;

use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_datetime;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function rtrim;

/**
 * @type SFTPConfiguration = array{protocol: string, client: SFTPClientConfiguration, options: SFTPOptions}
 * @import-type SFTPOptions from Options
 * @import-type SFTPClientConfiguration from ConfiguredSFTPClient
 *
 */
final readonly class SFTPFilesystem implements Filesystem
{
    public function __construct(
        private Mount $mount,
        private SFTPClientSource $client,
        private Options $options = new Options(),
    ) {}

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        return new self(
            new Mount(type_string()->assert($configuration['protocol'] ?? null)),
            ConfiguredSFTPClient::fromArray(type_array()->assert($configuration['client'] ?? null)),
            Options::fromArray(type_array()->assert($configuration['options'] ?? null)),
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        $this->guardWritable($path);
        $this->createParentDirectory($path);

        return SFTPDestinationStream::openAppend(
            $this->client->client(),
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize(),
        );
    }

    /**
     * @return null|SFTPConfiguration
     */
    public function normalize(): ?array
    {
        $client = $this->client->normalize();
        $options = $this->options->normalize();

        if ($client === null || $options === null) {
            return null;
        }

        return ['protocol' => $this->mount->protocol, 'client' => $client, 'options' => $options];
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
                if ($this->client->client()->is_file($remotePath)) {
                    $fileStatus = new FileStatus(
                        $path,
                        true,
                        type_integer()->assert($this->client->client()->filesize($remotePath)),
                        type_datetime()->cast($this->client->client()->filemtime($remotePath)),
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

        foreach ((new DirectoryListing($this->client->client()))->read($root) as $entry) {
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
            if (!$this->client->client()->file_exists($remoteFrom)) {
                return false;
            }

            $this->createParentDirectory($to);

            $this->client->client()->delete($remoteTo, true);
            $this->client->client()->rename($remoteFrom, $remoteTo);
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

        return new SFTPSourceStream($path, $this->client->client(), $this->options);
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
                if (!$this->client->client()->file_exists($remotePath)) {
                    continue;
                }

                $this->client->client()->delete($remotePath, true);
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
            if ($this->client->client()->is_dir($path->path())) {
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
            $this->client->client(),
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize(),
        );
    }

    private function createParentDirectory(Path $path): void
    {
        $parent = $path->parentDirectory()->path();

        try {
            if ($this->client->client()->is_dir($parent)) {
                return;
            }

            $this->client->client()->mkdir($parent, recursive: true);
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

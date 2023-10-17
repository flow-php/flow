<?php

declare(strict_types=1);

namespace Flow\ETL\Filesystem;

use Aws\S3\S3Client;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\MissingDependencyException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Filesystem;
use Flow\ETL\Filesystem\Stream\FileStream;
use Flow\ETL\Filesystem\Stream\Mode;
use Flow\ETL\Partition\NoopFilter;
use Flow\ETL\Partition\PartitionFilter;
use League\Flysystem\AwsS3V3\AwsS3V3Adapter;
use League\Flysystem\AzureBlobStorage\AzureBlobStorageAdapter;
use League\Flysystem\DirectoryAttributes;
use League\Flysystem\FileAttributes;
use League\Flysystem\Filesystem as Flysystem;
use League\Flysystem\FilesystemException;
use League\Flysystem\Local\LocalFilesystemAdapter;
use MicrosoftAzure\Storage\Blob\BlobRestProxy;

/**
 * @implements Filesystem<array<mixed>>
 */
final class FlysystemFS implements Filesystem
{
    public function __serialize() : array
    {
        return [];
    }

    public function __unserialize(array $data) : void
    {
    }

    public function directoryExists(Path $path) : bool
    {
        $fs = match ($path->scheme()) {
            AwsS3Stream::PROTOCOL => $this->aws($path),
            AzureBlobStream::PROTOCOL => $this->azure($path),
            'file' => $this->local(),
            default => throw new InvalidArgumentException('Unexpected scheme: ' . $path->scheme())
        };

        if ($path->isPattern()) {
            return false;
        }

        return $fs->directoryExists($path->path());
    }

    /**
     * @psalm-suppress UnusedForeachValue
     */
    public function exists(Path $path) : bool
    {
        $fs = match ($path->scheme()) {
            AwsS3Stream::PROTOCOL => $this->aws($path),
            AzureBlobStream::PROTOCOL => $this->azure($path),
            'file' => $this->local(),
            default => throw new InvalidArgumentException('Unexpected scheme: ' . $path->scheme())
        };

        if ($path->isPattern()) {
            $anyFileExistsInPattern = false;

            /** @psalm-suppress UnusedForeachValue */
            foreach ($this->scan($path, new NoopFilter()) as $nextPath) {
                $anyFileExistsInPattern = true;

                break;
            }

            return $anyFileExistsInPattern;
        }

        return $fs->fileExists($path->path()) || $fs->directoryExists($path->path());
    }

    /**
     * @psalm-suppress UnusedForeachValue
     */
    public function fileExists(Path $path) : bool
    {
        $fs = match ($path->scheme()) {
            AwsS3Stream::PROTOCOL => $this->aws($path),
            AzureBlobStream::PROTOCOL => $this->azure($path),
            'file' => $this->local(),
            default => throw new InvalidArgumentException('Unexpected scheme: ' . $path->scheme())
        };

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
            $fs = $this->local();

            if (!$fs->directoryExists($path->parentDirectory()->path())) {
                $fs->createDirectory($path->parentDirectory()->path());
            }

            return new FileStream($path, \fopen($path->path(), $mode->value, false, $path->context()->resource()) ?: null);
        }

        return new FileStream($path, \fopen($path->uri(), $mode->value, false, $path->context()->resource()) ?: null);
    }

    public function rm(Path $path) : void
    {
        $fs = match ($path->scheme()) {
            AwsS3Stream::PROTOCOL => $this->aws($path),
            AzureBlobStream::PROTOCOL => $this->azure($path),
            'file' => $this->local(),
            default => throw new InvalidArgumentException('Unexpected scheme: ' . $path->scheme())
        };

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
    public function scan(Path $path, PartitionFilter $partitionFilter) : \Generator
    {
        $fs = match ($path->scheme()) {
            AwsS3Stream::PROTOCOL => $this->aws($path),
            AzureBlobStream::PROTOCOL => $this->azure($path),
            'file' => $this->local(),
            default => throw new InvalidArgumentException('Unexpected scheme: ' . $path->scheme())
        };

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

            return $partitionFilter->keep(...(new Path(DIRECTORY_SEPARATOR . $file->path()))->partitions());
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

    /**
     * @throws MissingDependencyException
     * @throws InvalidArgumentException
     */
    private function aws(Path $path) : Flysystem
    {
        AwsS3Stream::register();

        $options = $path->options();

        $expectedOptions = '["client" => ["credentials" => ["key" => "__key__", "secret" => "__secret__"], "region" => "eu-west-2", "version" => "latest"], "bucket" => "__name__"]';

        if (!\array_key_exists('client', $options)) {
            throw new InvalidArgumentException("Missing AWS client in Path options, expected options: {$expectedOptions}");
        }

        if (!\array_key_exists('bucket', $options)) {
            throw new InvalidArgumentException("Missing AWS bucket in Path options, expected options: {$expectedOptions}");
        }

        /**
         * @psalm-suppress MixedArgument
         *
         * @phpstan-ignore-next-line
         */
        return new Flysystem(new AwsS3V3Adapter(new S3Client($options['client']), $options['bucket']));
    }

    /**
     * @param Path $path
     *
     * @return Flysystem
     */
    private function azure(Path $path) : Flysystem
    {
        AzureBlobStream::register();

        $options = $path->options();

        $expectedOptions = '["connection-string" => "__connection_string___", "container" => "__container__"]';

        if (!\array_key_exists('connection-string', $options)) {
            throw new InvalidArgumentException("Missing Azure Blob connection-string in Path options, expected options: {$expectedOptions}");
        }

        if (!\array_key_exists('container', $options)) {
            throw new InvalidArgumentException("Missing Azure Blob container in Path options, expected options: {$expectedOptions}");
        }

        /**
         * @psalm-suppress MixedArgument
         *
         * @phpstan-ignore-next-line
         */
        return new Flysystem(new AzureBlobStorageAdapter(BlobRestProxy::createBlobService($options['connection-string']), $options['container']));
    }

    private function local() : Flysystem
    {
        return new Flysystem(new LocalFilesystemAdapter(DIRECTORY_SEPARATOR, linkHandling: LocalFilesystemAdapter::SKIP_LINKS));
    }
}

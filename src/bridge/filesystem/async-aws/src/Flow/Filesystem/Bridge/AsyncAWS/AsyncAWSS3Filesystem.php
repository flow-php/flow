<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\Core\Configuration;
use AsyncAws\S3\Exception\NoSuchKeyException;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\InvalidArgumentException;
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
use SensitiveParameter;

use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function ltrim;
use function str_ends_with;
use function trim;

/**
 * @type S3Configuration = array{protocol: string, bucket: string, client: array<Configuration::OPTION_*, null|string>, options: S3Options}
 * @import-type S3Options from Options
 *
 */
final readonly class AsyncAWSS3Filesystem implements Filesystem
{
    public function __construct(
        private Mount $mount,
        private string $bucket,
        private S3ClientSource $client,
        private Options $options,
    ) {
        if ($bucket === '') {
            throw new InvalidArgumentException('Bucket name can not be empty');
        }
    }

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        /** @var array<Configuration::OPTION_*, null|string> $client */
        $client = type_array()->assert($configuration['client'] ?? null);

        return new self(
            new Mount(type_string()->assert($configuration['protocol'] ?? null)),
            type_string()->assert($configuration['bucket'] ?? null),
            new ConfiguredS3Client($client),
            Options::fromArray(type_array()->assert($configuration['options'] ?? null)),
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return AsyncAWSS3DestinationStream::openAppend(
            $this->client->client(),
            $this->bucket,
            $path,
            $this->options->blockFactory(),
            $this->options->partSize(),
        );
    }

    /**
     * @return null|S3Configuration
     */
    public function normalize(): ?array
    {
        $client = $this->client->normalize();
        $options = $this->options->normalize();

        if ($client === null || $options === null) {
            return null;
        }

        return [
            'protocol' => $this->mount->protocol,
            'bucket' => $this->bucket,
            'client' => $client,
            'options' => $options,
        ];
    }

    public function create(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return AsyncAWSS3DestinationStream::openExclusive(
            $this->client->client(),
            $this->bucket,
            $path,
            $this->options->blockFactory(),
            $this->options->partSize(),
        );
    }

    public function getSystemTmpDir(): Path
    {
        return $this->options->tmpDir();
    }

    /**
     * @return \Generator<FileStatus>
     */
    public function list(Path $path, Filter $pathFilter = new KeepAll()): Generator
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if (
            !$path->isPattern()
            && $this->options->fileFastPath()
            && $path->extension() !== false
            && !str_ends_with($path->path(), DIRECTORY_SEPARATOR)
        ) {
            try {
                $headObject = $this->client
                    ->client()
                    ->headObject([
                        'Bucket' => $this->bucket,
                        'Key' => ltrim($path->path(), DIRECTORY_SEPARATOR),
                    ]);
                $headObject->resolve();

                $fileStatus = new FileStatus(
                    $path,
                    true,
                    $headObject->getContentLength(),
                    $headObject->getLastModified(),
                );

                if ($pathFilter->accept($fileStatus)) {
                    yield $fileStatus;
                }

                return;
            } catch (NoSuchKeyException) {
                // Not a single object - fall through to listing.
            }
        }

        if ($path->isPattern()) {
            $prefix = ltrim($path->staticPart()->path(), DIRECTORY_SEPARATOR);
        } else {
            $prefix = ltrim($path->path(), DIRECTORY_SEPARATOR);
        }

        $continuationToken = null;

        do {
            $result = $this->client
                ->client()
                ->listObjectsV2([
                    'Bucket' => $this->bucket,
                    'Prefix' => $prefix,
                    'ContinuationToken' => $continuationToken,
                ]);

            foreach ($result->getContents() as $object) {
                $objectPath = path(
                    $path->protocol() . '://' . DIRECTORY_SEPARATOR
                        . ltrim((string) $object->getKey(), DIRECTORY_SEPARATOR),
                    $path->options(),
                );
                $objectFileStatus = new FileStatus(
                    $objectPath,
                    (bool) $objectPath->extension(),
                    $object->getSize(),
                    $object->getLastModified(),
                );

                if ($path->isPattern() && !$path->matches($objectPath)) {
                    continue;
                }

                if ($pathFilter->accept($objectFileStatus)) {
                    yield $objectFileStatus;
                }
            }

            $continuationToken = $result->getNextContinuationToken();
        } while ($continuationToken);
    }

    public function mount(): Mount
    {
        return $this->mount;
    }

    public function mv(Path $from, Path $to): bool
    {
        $this->mount->supports($from) || throw new InvalidSchemeException($from->protocol(), $this->mount->protocol);
        $this->mount->supports($to) || throw new InvalidSchemeException($to->protocol(), $this->mount->protocol);

        $this->client
            ->client()
            ->copyObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($to->path(), '/'),
                'CopySource' => $this->bucket . '/' . ltrim($from->path(), '/'),
            ]);

        $this->client
            ->client()
            ->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($from->path(), '/'),
            ]);

        return true;
    }

    public function readFrom(Path $path): SourceStream
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return new AsyncAWSS3SourceStream($path, $this->bucket, $this->client->client());
    }

    public function rm(Path $path, bool $recursive = true): bool
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return false;
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if ($path->isPattern()) {
            $deletedCount = 0;

            foreach ($this->list($path) as $fileStatus) {
                $this->client
                    ->client()
                    ->deleteObject([
                        'Bucket' => $this->bucket,
                        'Key' => ltrim($fileStatus->path->path(), '/'),
                    ]);

                $deletedCount++;
            }

            return (bool) $deletedCount;
        }

        try {
            $headObject = $this->client
                ->client()
                ->headObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim($path->path(), '/'),
                ]);
            $headObject->resolve();
        } catch (NoSuchKeyException) {
            /**
             * Since AzureS3 doesn't have a concept of folders, before we check if the intention is not to delete
             * entire path, like for example aws-s3://nested/folder we need to first add / at the end, to accidentally
             * not delete files that would also match the prefix, like: aws-s3://nested/folder_but_file.txt.
             */
            $folderPath = path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

            if (!$recursive) {
                foreach ($this->list($folderPath) as $fileStatus) {
                    if ($fileStatus->path->path() !== $folderPath->path()) {
                        throw new RuntimeException("Directory {$path->uri()} was not removed: it is not empty");
                    }
                }
            }

            $deletedCount = 0;

            foreach ($this->list($folderPath) as $fileStatus) {
                $this->client
                    ->client()
                    ->deleteObject([
                        'Bucket' => $this->bucket,
                        'Key' => ltrim($fileStatus->path->path(), '/'),
                    ]);
                $deletedCount++;
            }

            return (bool) $deletedCount;
        }

        $this->client
            ->client()
            ->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($path->path(), '/'),
            ]);

        return true;
    }

    public function status(Path $path): ?FileStatus
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            return new FileStatus($path, false);
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        if (!$path->isPattern()) {
            if ($path->path() === '/') {
                return new FileStatus($path, false);
            }

            try {
                $headObject = $this->client
                    ->client()
                    ->headObject([
                        'Bucket' => $this->bucket,
                        'Key' => ltrim($path->path(), '/'),
                    ]);
                $headObject->resolve();

                return new FileStatus($path, true, $headObject->getContentLength(), $headObject->getLastModified());
            } catch (NoSuchKeyException) {
                /**
                 * Since S3 doesn't have a concept of folders, before we check if the intention is not to delete
                 * entire path, like for example aws-s3://nested/folder we need to first add / at the end, to accidentally
                 * not match files that would also match the prefix, like: aws-s3://nested/folder_but_file.txt.
                 */
                $folderPath = path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

                foreach ($this->list($folderPath) as $fileStatus) {
                    return new FileStatus($folderPath, false);
                }

                return null;
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
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return AsyncAWSS3DestinationStream::openBlank(
            $this->client->client(),
            $this->bucket,
            $path,
            $this->options->blockFactory(),
            $this->options->partSize(),
        );
    }
}

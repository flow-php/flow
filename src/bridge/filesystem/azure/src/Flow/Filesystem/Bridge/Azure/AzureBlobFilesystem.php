<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Exception;
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
use SensitiveParameter;

use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function ltrim;
use function str_ends_with;
use function trim;

/**
 * @type AzureConfiguration = array{protocol: string, blob_service: BlobServiceConfiguration, options: AzureOptions}
 * @import-type AzureOptions from Options
 * @import-type BlobServiceConfiguration from ConfiguredBlobService
 *
 */
final readonly class AzureBlobFilesystem implements Filesystem
{
    public function __construct(
        private Mount $mount,
        private BlobServiceSource $blobService,
        private Options $options,
    ) {}

    public static function fromArray(#[SensitiveParameter] array $configuration): static
    {
        return new self(
            new Mount(type_string()->assert($configuration['protocol'] ?? null)),
            ConfiguredBlobService::fromArray(type_array()->assert($configuration['blob_service'] ?? null)),
            Options::fromArray(type_array()->assert($configuration['options'] ?? null)),
        );
    }

    public function appendTo(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return AzureBlobDestinationStream::openAppend(
            $this->blobService->blobService(),
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize(),
        );
    }

    /**
     * @return null|AzureConfiguration
     */
    public function normalize(): ?array
    {
        $blobService = $this->blobService->normalize();
        $options = $this->options->normalize();

        if ($blobService === null || $options === null) {
            return null;
        }

        return ['protocol' => $this->mount->protocol, 'blob_service' => $blobService, 'options' => $options];
    }

    public function create(Path $path): DestinationStream
    {
        if ($path->isEqual($this->getSystemTmpDir())) {
            throw new RuntimeException('Cannot write to system tmp directory');
        }

        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return AzureBlobDestinationStream::openExclusive(
            $this->blobService->blobService(),
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
            $blobProperties = $this->blobService
                ->blobService()
                ->getBlobProperties(ltrim($path->path(), DIRECTORY_SEPARATOR));

            if ($blobProperties !== null) {
                $fileStatus = new FileStatus($path, true, $blobProperties->size(), $blobProperties->lastModifiedAt());

                if ($pathFilter->accept($fileStatus)) {
                    yield $fileStatus;
                }

                return;
            }
        }

        if ($path->isPattern()) {
            $prefix = ltrim($path->staticPart()->path(), DIRECTORY_SEPARATOR);
        } else {
            $prefix = ltrim($path->path(), DIRECTORY_SEPARATOR);
        }

        $options = $this->options->listBlobOptions();

        if ($prefix) {
            $options->withPrefix($prefix);
        }

        foreach ($this->blobService->blobService()->listBlobs($options) as $blob) {
            $blobPath = path(
                $path->protocol() . '://' . DIRECTORY_SEPARATOR . ltrim($blob->name(), DIRECTORY_SEPARATOR),
                $path->options(),
            );
            $blobFileStatus = new FileStatus(
                $blobPath,
                (bool) $blobPath->extension(),
                $blob->size(),
                $blob->lastModifiedAt(),
            );

            if ($path->isPattern() && !$path->matches($blobPath)) {
                continue;
            }

            if ($pathFilter->accept($blobFileStatus)) {
                yield $blobFileStatus;
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

        $this->blobService->blobService()->copyBlob($from->path(), $to->path());
        $this->blobService->blobService()->deleteBlob($from->path());

        return true;
    }

    public function readFrom(Path $path): SourceStream
    {
        $this->mount->supports($path) || throw new InvalidSchemeException($path->protocol(), $this->mount->protocol);

        return new AzureBlobSourceStream($path, $this->blobService->blobService());
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
                $this->blobService->blobService()->deleteBlob($fileStatus->path->path());
                $deletedCount++;
            }

            return (bool) $deletedCount;
        }

        try {
            $this->blobService->blobService()->deleteBlob($path->path());

            return true;
        } catch (Exception) {
            /**
             * Since AzureBlobStorage doesn't have a concept of folders, before we check if the intention is not to delete
             * entire path, like for example azure-blob://nested/folder we need to first add / at the end, to accidentally
             * not delete files that would also match the prefix, like: azure-blob://nested/folder_but_file.txt.
             */
            $folderPath = path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

            if (!$recursive && $this->list($folderPath)->valid()) {
                throw new RuntimeException("Directory {$path->uri()} was not removed: it is not empty");
            }

            $deletedCount = 0;

            foreach ($this->list($folderPath) as $fileStatus) {
                $this->blobService->blobService()->deleteBlob($fileStatus->path->path());
                $deletedCount++;
            }

            return (bool) $deletedCount;
        }
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

            $blobProperties = $this->blobService
                ->blobService()
                ->getBlobProperties(ltrim($path->path(), DIRECTORY_SEPARATOR));

            if ($blobProperties === null) {
                /**
                 * Since AzureBlobStorage doesn't have a concept of folders, before we check if the intention is not to delete
                 * entire path, like for example azure-blob://nested/folder we need to first add / at the end, to accidentally
                 * not match files that would also match the prefix, like: azure-blob://nested/folder_but_file.txt.
                 */
                $folderPath = path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

                foreach ($this->list($folderPath) as $fileStatus) {
                    return new FileStatus($folderPath, false);
                }

                return null;
            }

            return new FileStatus($path, true, $blobProperties->size(), $blobProperties->lastModifiedAt());
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

        return AzureBlobDestinationStream::openBlank(
            $this->blobService->blobService(),
            $path,
            $this->options->blockFactory(),
            $this->options->blockSize(),
        );
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobService\BlockBlob\Block;
use Flow\Azure\SDK\BlobService\BlockBlob\BlockList;
use Flow\Azure\SDK\BlobService\BlockBlob\BlockState;
use Flow\Azure\SDK\BlobService\PutBlockBlobBlockList\PutBlockBlobBlockListOptions;
use Flow\Azure\SDK\Exception\AzureException;
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

use function bin2hex;
use function ceil;
use function count;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_string;
use function ltrim;
use function max;
use function min;
use function random_bytes;
use function rawurldecode;
use function sprintf;
use function str_ends_with;
use function strlen;
use function trim;

/**
 * @type AzureConfiguration = array{protocol: string, blob_service: BlobServiceConfiguration, options: AzureOptions}
 * @import-type AzureOptions from Options
 * @import-type BlobServiceConfiguration from ConfiguredBlobService
 *
 */
final readonly class AzureBlobFilesystem implements Filesystem
{
    private const int AZURE_MAX_BLOCK_SIZE = 4000 * 1024 * 1024;

    private const int AZURE_MAX_BLOCKS = 50_000;

    private const int BLOB_BATCH_LIMIT = 256;

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

        if ($from->isEqual($to)) {
            return true;
        }

        $service = $this->blobService->blobService();
        $source = ltrim($from->path(), DIRECTORY_SEPARATOR);
        $target = ltrim($to->path(), DIRECTORY_SEPARATOR);
        $properties = $service->getBlobProperties($source) ?? throw new RuntimeException(sprintf(
            'Cannot move "%s" to "%s", because "%s" does not exist',
            $from->uri(),
            $to->uri(),
            $from->uri(),
        ));
        $size = $properties->size();

        if ($size <= $this->options->copyPartSize()) {
            $service->copyBlob($source, $target);
        } else {
            $part = min(
                max($this->options->copyPartSize(), (int) ceil($size / self::AZURE_MAX_BLOCKS)),
                self::AZURE_MAX_BLOCK_SIZE,
            );
            $blocks = [];

            for ($first = 0; $first < $size; $first += $part) {
                $blockId = bin2hex(random_bytes(16));
                $service->putBlockBlobBlockFromBlob($target, $blockId, $source, $first, min($first + $part, $size) - 1);
                $blocks[] = new Block($blockId, BlockState::UNCOMMITTED);
            }

            $service->putBlockBlobBlockList(
                $target,
                new BlockList(...$blocks),
                (new PutBlockBlobBlockListOptions())
                    ->withContentType($properties->contentType())
                    ->withContentEncoding($properties->contentEncoding())
                    ->withContentLanguage($properties->contentLanguage())
                    ->withCacheControl($properties->cacheControl())
                    ->withContentDisposition($properties->contentDisposition())
                    ->withMetadata($properties->metadata()),
            );
        }

        $service->deleteBlob($source);

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
            return $this->deleteListed($path);
        }

        try {
            $this->blobService->blobService()->deleteBlob(ltrim($path->path(), DIRECTORY_SEPARATOR));

            return true;
        } catch (AzureException $exception) {
            if ($exception->response->getStatusCode() !== 404) {
                throw $exception;
            }
        }

        // the trailing slash keeps "folder" from matching "folder_but_file.txt"
        $folderPath = path(trim($path->uri(), DIRECTORY_SEPARATOR) . DIRECTORY_SEPARATOR, $path->options());

        if (!$recursive && $this->list($folderPath)->valid()) {
            throw new RuntimeException("Directory {$path->uri()} was not removed: it is not empty");
        }

        return $this->deleteListed($folderPath);
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
                // the trailing slash keeps "folder" from matching "folder_but_file.txt"
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

    /**
     * @param non-empty-list<string> $blobs
     */
    private function deleteBlobs(Path $listed, array $blobs): void
    {
        try {
            $this->blobService->blobService()->deleteBlobs($blobs);
        } catch (AzureException $exception) {
            $requestPath = rawurldecode($exception->request->getUri()->getPath());
            $failedBlob = '';

            foreach ($blobs as $blob) {
                if (strlen($blob) > strlen($failedBlob) && str_ends_with($requestPath, '/' . $blob)) {
                    $failedBlob = $blob;
                }
            }

            if ($failedBlob === '') {
                throw $exception;
            }

            throw new RuntimeException(
                sprintf(
                    '%d of %d paths were not removed, the first is %s: %s',
                    $exception->failedRequests,
                    count($blobs),
                    path($listed->protocol() . '://' . DIRECTORY_SEPARATOR . $failedBlob, $listed->options())->uri(),
                    $exception->response->getHeaderLine('x-ms-error-code'),
                ),
                previous: $exception,
            );
        }
    }

    private function deleteListed(Path $path): bool
    {
        $blobs = [];
        $deleted = false;

        foreach ($this->list($path) as $fileStatus) {
            $blob = ltrim($fileStatus->path->path(), DIRECTORY_SEPARATOR);
            $deleted = true;

            if (!$this->options->bulkDelete()) {
                $this->blobService->blobService()->deleteBlob($blob);

                continue;
            }

            $blobs[] = $blob;

            if (count($blobs) === self::BLOB_BATCH_LIMIT) {
                $this->deleteBlobs($path, $blobs);
                $blobs = [];
            }
        }

        if ($blobs !== []) {
            $this->deleteBlobs($path, $blobs);
        }

        return $deleted;
    }
}

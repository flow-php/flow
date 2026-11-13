<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\Core\Exception\Http\ClientException;
use AsyncAws\S3\Exception\NoSuchKeyException;
use AsyncAws\S3\Input\CreateMultipartUploadRequest;
use AsyncAws\S3\Result\HeadObjectOutput;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DesintationStream\AsyncAWSS3BlockLifecycle;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;
use Flow\Filesystem\Stream\Blocks;
use Throwable;

use function ceil;
use function fclose;
use function fopen;
use function gettype;
use function is_resource;
use function ltrim;
use function max;
use function rewind;
use function sprintf;
use function stream_get_meta_data;
use function unlink;

final class AsyncAWSS3DestinationStream implements DestinationStream
{
    private const int S3_MAX_COPY_PART_SIZE = 5_000_000_000;

    private const int S3_MAX_PARTS = 10000;

    private const int S3_MIN_PART_SIZE = 5 * 1024 * 1024;

    private bool $closed;

    private ?string $ifNoneMatch = null;

    /**
     * @var null|array{source: Path, first: int, last: int}
     */
    private ?array $pendingTail = null;

    public function __construct(
        private readonly S3Client $s3Client,
        private readonly string $uploadId,
        private readonly string $bucket,
        private readonly Path $path,
        private readonly Blocks $blocks,
        private readonly BlockList $blockList,
    ) {
        $this->closed = false;
    }

    /**
     * @param int<1, max> $blockSize
     */
    public static function openAppend(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = self::S3_MIN_PART_SIZE,
        int $copyPartSize = 256 * 1024 * 1024,
    ): self {
        try {
            $objectHead = $s3Client->headObject([
                'Bucket' => $bucket,
                'Key' => ltrim($path->path(), '/'),
            ]);
            $objectHead->resolve();
        } catch (NoSuchKeyException) {
            return self::openBlank($s3Client, $bucket, $path, $blockFactory, $blockSize);
        }

        return self::openCopy($s3Client, $bucket, $path, $objectHead, $path, $copyPartSize, $blockFactory, $blockSize);
    }

    /**
     * @param int<1, max> $blockSize
     */
    public static function openBlank(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = self::S3_MIN_PART_SIZE,
    ): self {
        return self::openUpload(
            $s3Client,
            $bucket,
            $path,
            ['ContentType' => (new ContentTypeDetector())->from($path)],
            $blockFactory,
            $blockSize,
        );
    }

    /**
     * @param int<1, max> $blockSize
     *
     * @throws InvalidArgumentException when the copy part size is outside 5 MiB and 5 GB
     * @throws RuntimeException when a part copy returns no ETag, the upload is aborted
     */
    public static function openCopy(
        S3Client $s3Client,
        string $bucket,
        Path $source,
        HeadObjectOutput $sourceHead,
        Path $target,
        int $copyPartSize,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = self::S3_MIN_PART_SIZE,
    ): self {
        if ($copyPartSize < self::S3_MIN_PART_SIZE || $copyPartSize > self::S3_MAX_COPY_PART_SIZE) {
            throw new InvalidArgumentException(sprintf(
                'Copy part size must be between 5 MiB and 5 GB, got %d bytes',
                $copyPartSize,
            ));
        }

        $stream = self::openUpload(
            $s3Client,
            $bucket,
            $target,
            [
                'ContentType' => $sourceHead->getContentType() ?? (new ContentTypeDetector())->from($target),
                'ContentEncoding' => $sourceHead->getContentEncoding(),
                'CacheControl' => $sourceHead->getCacheControl(),
                'ContentDisposition' => $sourceHead->getContentDisposition(),
                'ContentLanguage' => $sourceHead->getContentLanguage(),
                'Metadata' => $sourceHead->getMetadata(),
            ],
            $blockFactory,
            $blockSize,
        );
        $sourceSize = $sourceHead->getContentLength() ?? 0;
        $partSize = max($copyPartSize, (int) ceil($sourceSize / self::S3_MAX_PARTS));
        $first = 0;

        try {
            for (; ($first + $partSize) <= $sourceSize; $first += $partSize) {
                $stream->copyPart($source, $first, $first + $partSize - 1);
            }

            if (($sourceSize - $first) >= self::S3_MIN_PART_SIZE) {
                $stream->copyPart($source, $first, $sourceSize - 1);
            } elseif ($sourceSize > $first) {
                $stream->pendingTail = ['source' => $source, 'first' => $first, 'last' => $sourceSize - 1];
            }
        } catch (Throwable $e) {
            $stream->abort();

            throw $e;
        }

        return $stream;
    }

    /**
     * @param int<1, max> $blockSize
     */
    public static function openExclusive(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = self::S3_MIN_PART_SIZE,
    ): self {
        $stream = self::openBlank($s3Client, $bucket, $path, $blockFactory, $blockSize);
        $stream->ifNoneMatch = '*';

        return $stream;
    }

    /**
     * @param array{ContentType: string, ContentEncoding?: null|string, CacheControl?: null|string, ContentDisposition?: null|string, ContentLanguage?: null|string, Metadata?: array<string, string>} $objectAttributes
     * @param int<1, max> $blockSize
     */
    private static function openUpload(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        array $objectAttributes,
        BlockFactory $blockFactory,
        int $blockSize,
    ): self {
        $response = $s3Client->createMultipartUpload(new CreateMultipartUploadRequest([
            ...$objectAttributes,
            'Bucket' => $bucket,
            'Key' => ltrim($path->path(), '/'),
        ]));

        /** @var string $uploadId */
        $uploadId = $response->getUploadId();

        return new self(
            $s3Client,
            $uploadId,
            $bucket,
            $path,
            new Blocks(
                $blockSize,
                $blockFactory,
                new AsyncAWSS3BlockLifecycle($s3Client, $path, $bucket, $uploadId, $blockList = new BlockList()),
            ),
            $blockList,
        );
    }

    public function append(string $data): DestinationStream
    {
        $this->appendPendingTail();
        $this->blocks->append($data);

        return $this;
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        try {
            if ($this->pendingTail !== null) {
                $this->copyPart(...$this->pendingTail);
                $this->pendingTail = null;
            }

            if ($this->blockList->count() === 0 && $this->blocks->size() === 0) {
                $this->completeWithAnEmptyPart();
            } else {
                $this->blocks->done();
                $this->complete($this->blockList->toArray());
            }
        } catch (Throwable $e) {
            $this->abort();

            throw $e;
        }

        $this->closed = true;
    }

    public function fromResource($resource): DestinationStream
    {
        if (!is_resource($resource)) {
            throw new InvalidArgumentException(
                'DestinationStream::fromResource expects resource type, given: ' . gettype($resource),
            );
        }

        $meta = stream_get_meta_data($resource);

        if ($meta['seekable']) {
            rewind($resource);
        }

        $this->appendPendingTail();
        $this->blocks->fromResource($resource);

        return $this;
    }

    public function isOpen(): bool
    {
        return !$this->closed;
    }

    public function path(): Path
    {
        return $this->path;
    }

    private function abort(): void
    {
        $this->s3Client->abortMultipartUpload([
            'Bucket' => $this->bucket,
            'Key' => ltrim($this->path->path(), '/'),
            'UploadId' => $this->uploadId,
        ])->resolve();
        $this->closed = true;
    }

    private function appendPendingTail(): void
    {
        if ($this->pendingTail === null) {
            return;
        }

        ['source' => $source, 'first' => $first, 'last' => $last] = $this->pendingTail;

        $this->blocks->append(
            $this->s3Client
                ->getObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim($source->path(), '/'),
                    'Range' => "bytes={$first}-{$last}",
                ])
                ->getBody()
                ->getContentAsString(),
        );
        $this->pendingTail = null;
    }

    /**
     * @param array<array{PartNumber: int, ETag: null|string}> $parts
     *
     * @throws FileExistsException when the stream is exclusive and the key exists
     * @throws RuntimeException when the stream is exclusive and a write conflicts, or the upload returned no ETag
     */
    private function complete(array $parts): void
    {
        try {
            $etag = $this->s3Client->completeMultipartUpload([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
                'UploadId' => $this->uploadId,
                'IfNoneMatch' => $this->ifNoneMatch,
                'MultipartUpload' => ['Parts' => $parts],
            ])->getEtag();
        } catch (ClientException $e) {
            $status = $e->getResponse()->getStatusCode();

            if ($this->ifNoneMatch === null || $status !== 409 && $status !== 412) {
                throw $e;
            }

            if ($status === 409) {
                throw new RuntimeException(
                    "A conflicting write to {$this->path->uri()} aborted the upload, call create() again",
                );
            }

            throw new FileExistsException('File already exists: ' . $this->path->uri());
        }

        if ($etag === null) {
            throw new RuntimeException(sprintf(
                'S3 CompleteMultipartUpload of "%s" returned no ETag, the upload is aborted',
                $this->path->uri(),
            ));
        }
    }

    private function completeWithAnEmptyPart(): void
    {
        $handle = fopen($this->blocks->block()->path()->path(), 'rb');

        if ($handle === false) {
            throw new InvalidArgumentException('Cannot open file: ' . $this->blocks->block()->path()->path());
        }

        $partResponse = $this->s3Client->uploadPart([
            'Bucket' => $this->bucket,
            'Key' => ltrim($this->path->path(), '/'),
            'PartNumber' => 1,
            'UploadId' => $this->uploadId,
            'Body' => $handle,
        ]);

        fclose($handle);
        unlink($this->blocks->block()->path()->path());

        $this->complete([['PartNumber' => 1, 'ETag' => $partResponse->getEtag()]]);
    }

    /**
     * @throws RuntimeException when the part copy returns no ETag
     */
    private function copyPart(Path $source, int $first, int $last): void
    {
        $partNumber = $this->blockList->count() + 1;
        $etag = $this->s3Client
            ->uploadPartCopy([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
                'UploadId' => $this->uploadId,
                'PartNumber' => $partNumber,
                'CopySource' => $this->bucket . '/' . ltrim($source->path(), '/'),
                'CopySourceRange' =>
                    $first === 0 && ($last + 1) < self::S3_MIN_PART_SIZE ? null : "bytes={$first}-{$last}",
            ])
            ->getCopyPartResult()
            ?->getEtag();

        if ($etag === null) {
            throw new RuntimeException(sprintf(
                'S3 UploadPartCopy of part %d from "%s" to "%s" returned no ETag, the upload is aborted',
                $partNumber,
                $source->uri(),
                $this->path->uri(),
            ));
        }

        $this->blockList->add($etag);
    }
}

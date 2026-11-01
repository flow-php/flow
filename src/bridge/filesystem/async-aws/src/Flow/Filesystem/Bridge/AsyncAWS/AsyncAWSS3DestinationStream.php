<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\Core\Exception\Http\ClientException;
use AsyncAws\S3\Exception\NoSuchKeyException;
use AsyncAws\S3\Input\CreateMultipartUploadRequest;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DesintationStream\AsyncAWSS3BlockLifecycle;
use Flow\Filesystem\DestinationStream;
use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\SizeUnits;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\BlockFactory;
use Flow\Filesystem\Stream\Blocks;

use function fclose;
use function fopen;
use function gettype;
use function is_resource;
use function ltrim;
use function rewind;
use function str_replace;
use function stream_get_meta_data;
use function unlink;

final class AsyncAWSS3DestinationStream implements DestinationStream
{
    private const int S3_MIN_PART_SIZE = 5 * SizeUnits::MiB_SIZE;

    private bool $closed;

    private ?string $ifNoneMatch = null;

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
        int $blockSize = 1024 * 1024 * 4,
    ): self {
        try {
            $objectHead = $s3Client->headObject([
                'Bucket' => $bucket,
                'Key' => ltrim($path->path(), '/'),
            ]);
            $objectHead->resolve();

            $appendPath = $path->suffix('_flow_php_append_');

            $response = $s3Client->createMultipartUpload(new CreateMultipartUploadRequest([
                'Bucket' => $bucket,
                'Key' => ltrim($appendPath->path(), '/'),
                'ContentType' => (new ContentTypeDetector())->from($path),
            ]));

            /**
             * @var string $uploadId
             */
            $uploadId = $response->getUploadId();

            $contentLength = $objectHead->getContentLength() ?? 0;

            if ($contentLength < self::S3_MIN_PART_SIZE) {
                $blocks = new Blocks(
                    $blockSize,
                    $blockFactory,
                    new AsyncAWSS3BlockLifecycle(
                        $s3Client,
                        $appendPath,
                        $bucket,
                        $uploadId,
                        $blockList = new BlockList(),
                    ),
                );

                $blocks->append(
                    $s3Client
                        ->getObject(['Bucket' => $bucket, 'Key' => ltrim($path->path(), '/')])
                        ->getBody()
                        ->getContentAsString(),
                );

                return new self($s3Client, $uploadId, $bucket, $appendPath, $blocks, $blockList);
            }

            $partCopyResponse = $s3Client->uploadPartCopy([
                'Bucket' => $bucket,
                'Key' => ltrim($appendPath->path(), '/'),
                'UploadId' => $uploadId,
                'PartNumber' => 1,
                'CopySource' => $bucket . '/' . ltrim($path->path(), '/'),
            ]);

            $blockList = new BlockList();
            $blockList->add((string) $partCopyResponse->getCopyPartResult()?->getEtag());

            return new self(
                $s3Client,
                $uploadId,
                $bucket,
                $appendPath,
                new Blocks(
                    $blockSize,
                    $blockFactory,
                    new AsyncAWSS3BlockLifecycle($s3Client, $appendPath, $bucket, $uploadId, $blockList),
                ),
                $blockList,
            );
        } catch (NoSuchKeyException) {
            return self::openBlank($s3Client, $bucket, $path, $blockFactory, $blockSize);
        }
    }

    /**
     * @param int<1, max> $blockSize
     */
    public static function openBlank(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = 1024 * 1024 * 4,
    ): self {
        $response = $s3Client->createMultipartUpload(new CreateMultipartUploadRequest([
            'Bucket' => $bucket,
            'Key' => ltrim($path->path(), '/'),
            'ContentType' => (new ContentTypeDetector())->from($path),
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

    /**
     * @param int<1, max> $blockSize
     */
    public static function openExclusive(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = 1024 * 1024 * 4,
    ): self {
        $stream = self::openBlank($s3Client, $bucket, $path, $blockFactory, $blockSize);
        $stream->ifNoneMatch = '*';

        return $stream;
    }

    public function append(string $data): DestinationStream
    {
        $this->blocks->append($data);

        return $this;
    }

    public function close(): void
    {
        if ($this->closed) {
            return;
        }

        if ($this->blocks->size() === 0) {
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

            if ($this->path->endsWith('_flow_php_append_')) {
                $this->s3Client->deleteObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim(str_replace('/_flow_php_append_', '', $this->path->path()), '/'),
                ]);

                $this->s3Client->copyObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim(str_replace('/_flow_php_append_', '', $this->path->path()), '/'),
                    'CopySource' => $this->bucket . '/' . ltrim($this->path->path(), '/'),
                ]);

                $this->s3Client->deleteObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim($this->path->path(), '/'),
                ]);
            }

            $this->closed = true;

            return;
        }

        $this->blocks->done();

        $this->complete($this->blockList->toArray());

        if ($this->path->endsWith('_flow_php_append_')) {
            $this->s3Client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim(str_replace('/_flow_php_append_', '', $this->path->path()), '/'),
            ]);

            $this->s3Client->copyObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim(str_replace('/_flow_php_append_', '', $this->path->path()), '/'),
                'CopySource' => $this->bucket . '/' . ltrim($this->path->path(), '/'),
            ]);

            $this->s3Client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
            ]);
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

    /**
     * @param array<array{PartNumber: int, ETag: null|string}> $parts
     *
     * @throws FileExistsException when the stream is exclusive and the key exists
     * @throws RuntimeException when the stream is exclusive and a conflicting write aborted the upload
     */
    private function complete(array $parts): void
    {
        try {
            $this->s3Client->completeMultipartUpload([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
                'UploadId' => $this->uploadId,
                'IfNoneMatch' => $this->ifNoneMatch,
                'MultipartUpload' => ['Parts' => $parts],
            ])->resolve();
        } catch (ClientException $e) {
            $status = $e->getResponse()->getStatusCode();

            if ($this->ifNoneMatch === null || $status !== 409 && $status !== 412) {
                throw $e;
            }

            $this->s3Client->abortMultipartUpload([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
                'UploadId' => $this->uploadId,
            ])->resolve();
            $this->closed = true;

            if ($status === 409) {
                throw new RuntimeException(
                    "A conflicting write to {$this->path->uri()} aborted the upload, call create() again",
                );
            }

            throw new FileExistsException('File already exists: ' . $this->path->uri());
        }
    }
}

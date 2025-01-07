<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\Exception\NoSuchKeyException;
use AsyncAws\S3\Input\CreateMultipartUploadRequest;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DesintationStream\AsyncAWSS3BlockLifecycle;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\{BlockFactory, Blocks};
use Flow\Filesystem\{DestinationStream, Path, SizeUnits};

final class AsyncAWSS3DestinationStream implements DestinationStream
{
    private bool $closed;

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

    public static function openAppend(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = 1024 * 1024 * 4,
    ) : self {

        try {
            $objectHead = $s3Client->headObject([
                'Bucket' => $bucket,
                'Key' => \ltrim($path->path(), '/'),
            ]);
            $objectHead->resolve();

            $appendPath = $path->suffix('_flow_php_append_');

            $response = $s3Client->createMultipartUpload(new CreateMultipartUploadRequest([
                'Bucket' => $bucket,
                'Key' => \ltrim($appendPath->path(), '/'),
            ]));

            /**
             * @var string $uploadId
             */
            $uploadId = $response->getUploadId();

            /**
             * If the file is less than 5mb, we can just copy it to the new file
             * and append to it. We need to read the file to memory and append it to the new file.
             * S3 allows only the last part to be smaller than 5Mb, all other parts needs to be 5Mb+.
             */
            if ($objectHead->getContentLength() < SizeUnits::mbToBytes(5)) {
                $blocks = new Blocks(
                    $blockSize,
                    $blockFactory,
                    new AsyncAWSS3BlockLifecycle($s3Client, $appendPath, $bucket, $uploadId, $blockList = new BlockList())
                );

                $blocks->append(
                    $s3Client->getObject(['Bucket' => $bucket, 'Key' => ltrim($path->path(), '/')])
                    ->getBody()
                    ->getContentAsString()
                );

                return new self(
                    $s3Client,
                    $uploadId,
                    $bucket,
                    $appendPath,
                    $blocks,
                    $blockList
                );
            }

            $partCopyResponse = $s3Client->uploadPartCopy([
                'Bucket' => $bucket,
                'Key' => ltrim($appendPath->path(), '/'),
                'UploadId' => $uploadId,
                'PartNumber' => 1,
                'CopySource' => $bucket . '/' . ltrim($path->path(), '/'),
            ]);

            $blockList = new BlockList();
            $blockList->add($partCopyResponse->getCopyPartResult()?->getEtag());

            return new self(
                $s3Client,
                $uploadId,
                $bucket,
                $appendPath,
                new Blocks(
                    $blockSize,
                    $blockFactory,
                    new AsyncAWSS3BlockLifecycle($s3Client, $appendPath, $bucket, $uploadId, $blockList)
                ),
                $blockList
            );
        } catch (NoSuchKeyException) {
            return self::openBlank($s3Client, $bucket, $path, $blockFactory, $blockSize);
        }
    }

    public static function openBlank(
        S3Client $s3Client,
        string $bucket,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = 1024 * 1024 * 4,
    ) : self {

        $response = $s3Client->createMultipartUpload(new CreateMultipartUploadRequest([
            'Bucket' => $bucket,
            'Key' => \ltrim($path->path(), '/'),
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
                new AsyncAWSS3BlockLifecycle($s3Client, $path, $bucket, $uploadId, $blockList = new BlockList())
            ),
            $blockList
        );
    }

    public function append(string $data) : DestinationStream
    {
        $this->blocks->append($data);

        return $this;
    }

    public function close() : void
    {
        if ($this->blocks->size() === 0) {

            $handle = \fopen($this->blocks->block()->path()->path(), 'rb');

            $partResponse = $this->s3Client->uploadPart([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
                'PartNumber' => 1,
                'UploadId' => $this->uploadId,
                'Body' => $handle,
            ]);

            \fclose($handle);

            $this->s3Client->completeMultipartUpload([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
                'UploadId' => $this->uploadId,
                'MultipartUpload' => [
                    'Parts' => [
                        ['PartNumber' => 1, 'ETag' => $partResponse->getEtag()],
                    ],
                ],
            ]);

            if ($this->path->endsWith('_flow_php_append_')) {
                $this->s3Client->deleteObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim(\str_replace('_flow_php_append_', '', $this->path->path()), '/'),
                ]);

                $this->s3Client->copyObject([
                    'Bucket' => $this->bucket,
                    'Key' => ltrim(\str_replace('_flow_php_append_', '', $this->path->path()), '/'),
                    'CopySource' => $this->bucket . '/' . $this->path->path(),
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

        $this->s3Client->completeMultipartUpload([
            'Bucket' => $this->bucket,
            'Key' => ltrim($this->path->path(), '/'),
            'UploadId' => $this->uploadId,
            'MultipartUpload' => [
                'Parts' => $this->blockList->toArray(),
            ],
        ]);

        if ($this->path->endsWith('_flow_php_append_')) {
            $this->s3Client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim(\str_replace('/_flow_php_append_', '', $this->path->path()), '/'),
            ]);

            $this->s3Client->copyObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim(\str_replace('/_flow_php_append_', '', $this->path->path()), '/'),
                'CopySource' => $this->bucket . '/' . ltrim($this->path->path(), '/'),
            ]);

            $this->s3Client->deleteObject([
                'Bucket' => $this->bucket,
                'Key' => ltrim($this->path->path(), '/'),
            ]);
        }

        $this->closed = true;
    }

    public function fromResource($resource) : DestinationStream
    {
        if (!\is_resource($resource)) {
            throw new InvalidArgumentException('DestinationStream::fromResource expects resource type, given: ' . \gettype($resource));
        }

        $meta = \stream_get_meta_data($resource);

        if ($meta['seekable']) {
            \rewind($resource);
        }

        $this->blocks->fromResource($resource);

        return $this;
    }

    public function isOpen() : bool
    {
        return !$this->closed;
    }

    public function path() : Path
    {
        return $this->path;
    }
}

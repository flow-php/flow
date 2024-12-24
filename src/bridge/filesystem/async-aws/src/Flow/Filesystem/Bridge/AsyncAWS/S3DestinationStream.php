<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS;

use AsyncAws\S3\Input\CreateMultipartUploadRequest;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DesintationStream\AsyncAWSS3BlockLifecycle;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Stream\Block\NativeLocalFileBlocksFactory;
use Flow\Filesystem\Stream\{BlockFactory, Blocks};
use Flow\Filesystem\{DestinationStream, Path};

final class S3DestinationStream implements DestinationStream
{
    private bool $closed;

    public function __construct(
        private readonly S3Client $s3Client,
        private readonly string $uploadId,
        private readonly Path $path,
        private readonly Blocks $blocks,
        private readonly BlockList $blockList,
    ) {

        $this->closed = false;
    }

    public static function openBlank(
        S3Client $s3Client,
        Path $path,
        BlockFactory $blockFactory = new NativeLocalFileBlocksFactory(),
        int $blockSize = 1024 * 1024 * 4,
    ) : self {

        if ($path->rootDirectoryName() === null) {
            throw new InvalidArgumentException('Root directory name can not be empty for S3 destination stream since it represents bucket name. Example: aws-s3://bucket_name/file.txt');
        }

        $response = $s3Client->createMultipartUpload(new CreateMultipartUploadRequest([
            'Bucket' => $path->rootDirectoryName(),
            'Key' => $path->skipDirectories(1)?->path(),
        ]));

        return new self(
            $s3Client,
            $uploadId = $response->getUploadId(),
            $path,
            new Blocks(
                $blockSize,
                $blockFactory,
                new AsyncAWSS3BlockLifecycle($s3Client, $path, $uploadId, $blockList = new BlockList())
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
                'Bucket' => $this->path->rootDirectoryName(),
                'Key' => $this->path->skipDirectories(1)?->path(),
                'PartNumber' => 1,
                'UploadId' => $this->uploadId,
                'Body' => $handle,
            ]);

            \fclose($handle);
            $this->s3Client->completeMultipartUpload([
                'Bucket' => $this->path->rootDirectoryName(),
                'Key' => $this->path->skipDirectories(1)?->path(),
                'UploadId' => $this->uploadId,
                'MultipartUpload' => [
                    'Parts' => [
                        ['PartNumber' => 1, 'ETag' => $partResponse->getEtag()],
                    ],
                ],
            ]);
            $this->closed = true;

            return;
        }

        $this->blocks->done();

        $this->s3Client->completeMultipartUpload([
            'Bucket' => $this->path->rootDirectoryName(),
            'Key' => $this->path->skipDirectories(1)?->path(),
            'UploadId' => $this->uploadId,
            'MultipartUpload' => [
                'Parts' => $this->blockList->toArray(),
            ],
        ]);

        $this->closed = true;
    }

    public function fromResource($resource) : DestinationStream
    {
        // TODO: Implement fromResource() method.
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

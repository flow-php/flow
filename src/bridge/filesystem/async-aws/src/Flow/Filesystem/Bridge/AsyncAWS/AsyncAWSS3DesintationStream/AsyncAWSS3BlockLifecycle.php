<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DesintationStream;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\BlockList;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Path;
use Flow\Filesystem\Stream\Block;
use Flow\Filesystem\Stream\BlockLifecycle;

use function fclose;
use function fopen;
use function unlink;

final readonly class AsyncAWSS3BlockLifecycle implements BlockLifecycle
{
    public function __construct(
        private S3Client $s3Client,
        private Path $path,
        private string $bucket,
        private string $uploadId,
        private BlockList $blockList,
    ) {}

    public function filled(Block $block): void
    {
        $handle = fopen($block->path()->path(), 'rb');

        if ($handle === false) {
            throw new RuntimeException('Cannot open block file for reading');
        }

        $uploadPartResponse = $this->s3Client->uploadPart([
            'Bucket' => $this->bucket,
            'Key' => ltrim($this->path->path(), '/'),
            'PartNumber' => $this->blockList->count() + 1,
            'UploadId' => $this->uploadId,
            'Body' => $handle,
        ]);

        /**
         * @var string $etag
         */
        $etag = $uploadPartResponse->getETag();

        fclose($handle);

        unlink($block->path()->path());

        $this->blockList->add($etag);
    }
}

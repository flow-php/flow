<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Double;

use AsyncAws\S3\Result\CopyObjectOutput;
use AsyncAws\S3\Result\DeleteObjectOutput;
use AsyncAws\S3\Result\HeadObjectOutput;
use AsyncAws\S3\Result\ListObjectsV2Output;
use AsyncAws\S3\S3Client;

final class RecordingS3Client extends S3Client
{
    public int $copyObjectCount = 0;

    public int $deleteObjectCount = 0;

    public int $headObjectCount = 0;

    public int $listObjectsV2Count = 0;

    #[\Override]
    public function copyObject($input): CopyObjectOutput
    {
        $this->copyObjectCount++;

        return parent::copyObject($input);
    }

    #[\Override]
    public function deleteObject($input): DeleteObjectOutput
    {
        $this->deleteObjectCount++;

        return parent::deleteObject($input);
    }

    #[\Override]
    public function headObject($input): HeadObjectOutput
    {
        $this->headObjectCount++;

        return parent::headObject($input);
    }

    #[\Override]
    public function listObjectsV2($input): ListObjectsV2Output
    {
        $this->listObjectsV2Count++;

        return parent::listObjectsV2($input);
    }

    public function resetCounters(): void
    {
        $this->copyObjectCount = 0;
        $this->deleteObjectCount = 0;
        $this->headObjectCount = 0;
        $this->listObjectsV2Count = 0;
    }
}

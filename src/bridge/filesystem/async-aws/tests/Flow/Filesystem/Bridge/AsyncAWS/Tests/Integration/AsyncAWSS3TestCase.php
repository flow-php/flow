<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Integration;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_client;
use AsyncAws\S3\S3Client;
use Flow\ETL\Tests\Integration\IntegrationTestCase;

abstract class AsyncAWSS3TestCase extends IntegrationTestCase
{
    protected function setUp() : void
    {
        parent::setUp();

        $buckets = $this->s3Client()->listBuckets();

        foreach ($buckets->getBuckets() as $bucket) {
            $this->deleteBucketContents($this->s3Client(), $bucket->getName());
            $this->s3Client()->deleteBucket(['Bucket' => $bucket->getName()]);
        }

        $this->s3Client()->createBucket(['Bucket' => $this->bucket()]);
    }

    protected function tearDown() : void
    {
        parent::tearDown();

        $buckets = $this->s3Client()->listBuckets();

        foreach ($buckets->getBuckets() as $bucket) {
            $this->deleteBucketContents($this->s3Client(), $bucket->getName());
            $this->s3Client()->deleteBucket(['Bucket' => $bucket->getName()]);
        }
    }

    public function bucket() : string
    {
        return 'test-bucket';
    }

    public function s3Client() : S3Client
    {
        return aws_s3_client([
            'endpoint' => 'http://127.0.0.1:4566',
            'region' => 'us-east-1',
            'accessKeyId' => 'test',
            'accessKeySecret' => 'test',
        ]);
    }

    private function deleteBucketContents(S3Client $s3Client, string $bucket) : void
    {
        $objects = $s3Client->listObjectsV2(['Bucket' => $bucket]);

        foreach ($objects->getContents() as $object) {
            $s3Client->deleteObject(['Bucket' => $bucket, 'Key' => $object->getKey()]);
        }
    }
}

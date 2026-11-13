<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\Core\Exception\Http\ServerException;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3DestinationStream;
use Flow\Filesystem\Bridge\AsyncAWS\Tests\Mother\S3ClientMother;
use Flow\Filesystem\Exception\FileExistsException;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Flow\Filesystem\Exception\RuntimeException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use function end;
use function Flow\Filesystem\DSL\path;
use function fopen;
use function fwrite;
use function implode;

final class AsyncAWSS3DestinationStreamTest extends TestCase
{
    public function test_exclusive_stream_aborts_and_asks_to_create_again_on_a_conflicting_write(): void
    {
        $calls = [];
        $stream = AsyncAWSS3DestinationStream::openExclusive(
            S3ClientMother::completingWith(409, $calls),
            'bucket',
            path('aws-s3://claim.txt'),
        );
        $stream->append('second');

        try {
            $stream->close();
            static::fail('Expected the conflicting write to be refused');
        } catch (FileExistsException) {
            static::fail('A conflicting write is not an existing file');
        } catch (RuntimeException $e) {
            static::assertStringContainsString('call create() again', $e->getMessage());
        }

        static::assertSame('DELETE uploadId=upload-1', end($calls));
    }

    public function test_exclusive_stream_aborts_and_refuses_an_existing_key(): void
    {
        $calls = [];
        $stream = AsyncAWSS3DestinationStream::openExclusive(
            S3ClientMother::completingWith(412, $calls),
            'bucket',
            path('aws-s3://claim.txt'),
        );
        $stream->append('second');

        try {
            $stream->close();
            static::fail('Expected the existing key to be refused');
        } catch (FileExistsException) {
        }

        static::assertSame('DELETE uploadId=upload-1', end($calls));
    }

    public function test_append_builds_the_upload_on_the_target_key(): void
    {
        $calls = [];

        AsyncAWSS3DestinationStream::openAppend(
            S3ClientMother::copying(12 * 1024 * 1024, $calls),
            'bucket',
            path('aws-s3://dst.bin'),
            blockSize: 5 * 1024 * 1024,
            copyPartSize: 5 * 1024 * 1024,
        )
            ->append('tail')
            ->close();

        static::assertSame(
            [
                'HeadObject dst.bin',
                'CreateMultipartUpload dst.bin',
                'UploadPartCopy dst.bin bytes=0-5242879',
                'UploadPartCopy dst.bin bytes=5242880-10485759',
                'GetObject dst.bin bytes=10485760-12582911',
                'UploadPart dst.bin',
                'CompleteMultipartUpload dst.bin',
            ],
            $calls,
        );
        static::assertStringNotContainsString('_flow_php_append_', implode("\n", $calls));
    }

    public function test_append_failure_keeps_the_original(): void
    {
        $calls = [];
        $stream = AsyncAWSS3DestinationStream::openAppend(
            S3ClientMother::copyingWithStatusOn(12 * 1024 * 1024, 'CompleteMultipartUpload', 500, $calls),
            'bucket',
            path('aws-s3://dst.bin'),
            blockSize: 5 * 1024 * 1024,
            copyPartSize: 5 * 1024 * 1024,
        );

        try {
            $stream->close();
            static::fail('Expected the failed complete to fail the append');
        } catch (ServerException) {
        }

        static::assertSame('AbortMultipartUpload dst.bin', end($calls));
        static::assertNotContains('DeleteObject dst.bin', $calls);
    }

    public function test_append_without_bytes_to_an_object_under_5_mib_copies_it_whole(): void
    {
        $calls = [];

        AsyncAWSS3DestinationStream::openAppend(
            S3ClientMother::copying(1024, $calls),
            'bucket',
            path('aws-s3://dst.bin'),
            blockSize: 5 * 1024 * 1024,
        )->close();

        static::assertSame(
            [
                'HeadObject dst.bin',
                'CreateMultipartUpload dst.bin',
                'UploadPartCopy dst.bin',
                'CompleteMultipartUpload dst.bin',
            ],
            $calls,
        );
    }

    public function test_append_to_an_object_under_5_mib_reads_it_before_the_new_bytes(): void
    {
        $calls = [];

        AsyncAWSS3DestinationStream::openAppend(
            S3ClientMother::copying(1024, $calls),
            'bucket',
            path('aws-s3://dst.bin'),
            blockSize: 5 * 1024 * 1024,
        )
            ->append('tail')
            ->close();

        static::assertSame(
            [
                'HeadObject dst.bin',
                'CreateMultipartUpload dst.bin',
                'GetObject dst.bin bytes=0-1023',
                'UploadPart dst.bin',
                'CompleteMultipartUpload dst.bin',
            ],
            $calls,
        );
    }

    public function test_append_from_a_resource_reads_the_tail_first(): void
    {
        $calls = [];
        $resource = fopen('php://memory', 'r+b');
        static::assertNotFalse($resource);
        fwrite($resource, 'tail');

        AsyncAWSS3DestinationStream::openAppend(
            S3ClientMother::copying(1024, $calls),
            'bucket',
            path('aws-s3://dst.bin'),
            blockSize: 5 * 1024 * 1024,
        )
            ->fromResource($resource)
            ->close();

        static::assertSame('GetObject dst.bin bytes=0-1023', $calls[2]);
        static::assertSame('UploadPart dst.bin', $calls[3]);
    }

    public function test_copy_remainder_of_5_mib_or_more_is_one_more_ranged_part(): void
    {
        $calls = [];
        $s3Client = S3ClientMother::copying(11 * 1024 * 1024, $calls);

        AsyncAWSS3DestinationStream::openCopy(
            $s3Client,
            'bucket',
            path('aws-s3://src.bin'),
            $s3Client->headObject(['Bucket' => 'bucket', 'Key' => 'src.bin']),
            path('aws-s3://dst.bin'),
            6 * 1024 * 1024,
        )->close();

        static::assertSame(
            [
                'HeadObject src.bin',
                'CreateMultipartUpload dst.bin',
                'UploadPartCopy dst.bin bytes=0-6291455',
                'UploadPartCopy dst.bin bytes=6291456-11534335',
                'CompleteMultipartUpload dst.bin',
            ],
            $calls,
        );
    }

    public function test_copy_above_10000_parts_grows_the_part_size(): void
    {
        $calls = [];
        $sourceSize = (5 * 1024 * 1024 * 10000) + 1;
        $s3Client = S3ClientMother::copying($sourceSize, $calls);

        AsyncAWSS3DestinationStream::openCopy(
            $s3Client,
            'bucket',
            path('aws-s3://src.bin'),
            $s3Client->headObject(['Bucket' => 'bucket', 'Key' => 'src.bin']),
            path('aws-s3://dst.bin'),
            5 * 1024 * 1024,
        )->close();

        static::assertCount(10003, $calls);
        static::assertSame('UploadPartCopy dst.bin bytes=0-5242880', $calls[2]);
        static::assertSame('UploadPartCopy dst.bin bytes=52423567119-' . ($sourceSize - 1), $calls[10001]);
    }

    public function test_copy_carries_the_object_attributes_of_the_source_head(): void
    {
        $uploadHeaders = [];
        $s3Client = S3ClientMother::copyingObjectWith(
            1024,
            [
                'content-type' => 'application/x-ndjson',
                'content-encoding' => 'gzip',
                'cache-control' => 'max-age=60',
                'content-disposition' => 'attachment',
                'content-language' => 'pl',
                'x-amz-meta-owner' => 'flow',
            ],
            $uploadHeaders,
        );

        AsyncAWSS3DestinationStream::openCopy(
            $s3Client,
            'bucket',
            path('aws-s3://src.bin'),
            $s3Client->headObject(['Bucket' => 'bucket', 'Key' => 'src.bin']),
            path('aws-s3://dst.csv'),
            5 * 1024 * 1024,
        )->close();

        static::assertEquals(
            [
                'content-type' => 'application/x-ndjson',
                'content-encoding' => 'gzip',
                'cache-control' => 'max-age=60',
                'content-disposition' => 'attachment',
                'content-language' => 'pl',
                'x-amz-meta-owner' => 'flow',
            ],
            $uploadHeaders,
        );
    }

    public function test_copy_of_a_source_without_content_type_detects_it_from_the_target(): void
    {
        $uploadHeaders = [];
        $s3Client = S3ClientMother::copyingObjectWith(1024, [], $uploadHeaders);

        AsyncAWSS3DestinationStream::openCopy(
            $s3Client,
            'bucket',
            path('aws-s3://src.bin'),
            $s3Client->headObject(['Bucket' => 'bucket', 'Key' => 'src.bin']),
            path('aws-s3://dst.csv'),
            5 * 1024 * 1024,
        )->close();

        static::assertSame(['content-type' => 'text/csv'], $uploadHeaders);
    }

    #[TestWith([5242879])]
    #[TestWith([5000000001])]
    public function test_copy_part_size_outside_5_mib_and_5_gb_is_rejected_before_any_upload(int $copyPartSize): void
    {
        $calls = [];
        $s3Client = S3ClientMother::copying(1024, $calls);

        try {
            AsyncAWSS3DestinationStream::openCopy(
                $s3Client,
                'bucket',
                path('aws-s3://src.bin'),
                $s3Client->headObject(['Bucket' => 'bucket', 'Key' => 'src.bin']),
                path('aws-s3://dst.bin'),
                $copyPartSize,
            );
            static::fail('Expected the copy part size to be rejected');
        } catch (InvalidArgumentException $e) {
            static::assertSame(
                "Copy part size must be between 5 MiB and 5 GB, got {$copyPartSize} bytes",
                $e->getMessage(),
            );
        }

        static::assertSame(['HeadObject src.bin'], $calls);
    }
}

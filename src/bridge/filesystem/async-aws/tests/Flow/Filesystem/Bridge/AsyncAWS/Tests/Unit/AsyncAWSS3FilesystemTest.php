<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\Core\Exception\Http\ServerException;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Bridge\AsyncAWS\Tests\Mother\S3ClientMother;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Exception\RuntimeException;
use Flow\Filesystem\Stream\BlockFactory;
use Flow\Types\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

use function end;
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem_options;
use function Flow\Filesystem\DSL\path;
use function iterator_to_array;

final class AsyncAWSS3FilesystemTest extends TestCase
{
    public function test_append_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);
        $this->expectExceptionMessage(
            'Scheme "file://" is not supported by this protocol. Expected scheme is "aws-s3://"',
        );

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->appendTo(path('file:///var/foo.txt'));
    }

    public function test_list_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        iterator_to_array(aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->list(path(
            'file:///var/foo.txt',
        )));
    }

    public function test_mv_rejects_mismatched_destination_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->mv(
            path('aws-s3://var/a.txt'),
            path('file:///var/b.txt'),
        );
    }

    public function test_mv_rejects_mismatched_source_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->mv(
            path('file:///var/a.txt'),
            path('aws-s3://var/b.txt'),
        );
    }

    public function test_read_from_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->readFrom(path('file:///var/foo.txt'));
    }

    public function test_rm_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->rm(path('file:///var/foo.txt'));
    }

    public function test_status_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->status(path('file:///var/foo.txt'));
    }

    public function test_supports_only_its_own_scheme(): void
    {
        $filesystem = aws_s3_filesystem('bucket', $this->createStub(S3Client::class));

        static::assertTrue($filesystem->supports(path('aws-s3://bucket/orders.csv')));
        static::assertFalse($filesystem->supports(path('file:///var/foo.txt')));
    }

    public function test_write_to_rejects_mismatched_scheme(): void
    {
        $this->expectException(InvalidSchemeException::class);

        aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->writeTo(path('file:///var/foo.txt'));
    }

    public function test_a_configured_filesystem_round_trips_its_configuration(): void
    {
        $filesystem = aws_s3_filesystem(
            'bucket',
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's'],
            protocol: 'warehouse',
        );
        $configuration = $filesystem->normalize();

        static::assertNotNull($configuration);
        static::assertSame('warehouse', $configuration['protocol']);
        static::assertSame('bucket', $configuration['bucket']);
        static::assertSame(
            ['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's'],
            $configuration['client'],
        );
        static::assertSame($configuration, AsyncAWSS3Filesystem::fromArray($configuration)->normalize());
    }

    public function test_a_filesystem_over_a_live_client_has_no_configuration(): void
    {
        static::assertNull(aws_s3_filesystem('bucket', $this->createStub(S3Client::class))->normalize());
    }

    public function test_a_filesystem_with_a_custom_block_factory_has_no_configuration(): void
    {
        static::assertNull(
            aws_s3_filesystem(
                'bucket',
                ['region' => 'eu-west-1'],
                (new Options())->withBlockFactory($this->createStub(BlockFactory::class)),
            )->normalize(),
        );
    }

    public function test_a_configuration_without_a_bucket_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);

        AsyncAWSS3Filesystem::fromArray(['protocol' => 'aws-s3', 'client' => [], 'options' => []]);
    }

    public function test_description_round_trip_keeps_http_limits_and_copy_part_size(): void
    {
        $configuration = aws_s3_filesystem(
            'bucket',
            ['region' => 'eu-west-1', 'http' => ['timeout' => 1, 'max_duration' => 2, 'max_retries' => 0]],
            aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
        )->normalize();

        static::assertNotNull($configuration);
        static::assertSame(
            ['timeout' => 1, 'max_duration' => 2, 'max_retries' => 0],
            $configuration['client']['http'] ?? null,
        );
        static::assertSame(5242880, $configuration['options']['copy_part_size']);
        static::assertSame($configuration, AsyncAWSS3Filesystem::fromArray($configuration)->normalize());
    }

    public function test_mv_above_copy_part_size_copies_in_ranged_parts(): void
    {
        $calls = [];

        aws_s3_filesystem(
            'bucket',
            S3ClientMother::copying((5 * 1024 * 1024 * 2) + 1, $calls),
            aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
        )->mv(path('aws-s3://src'), path('aws-s3://dst'));

        static::assertSame(
            [
                'HeadObject src',
                'CreateMultipartUpload dst',
                'UploadPartCopy dst bytes=0-5242879',
                'UploadPartCopy dst bytes=5242880-10485759',
                'UploadPartCopy dst bytes=10485760-10485760',
                'CompleteMultipartUpload dst',
                'DeleteObject src',
            ],
            $calls,
        );
    }

    public function test_mv_onto_itself_sends_no_request(): void
    {
        $calls = [];

        static::assertTrue(aws_s3_filesystem(
            'bucket',
            S3ClientMother::copying(12 * 1024 * 1024, $calls),
            aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
        )->mv(path('aws-s3://a.bin'), path('aws-s3://a.bin')));
        static::assertSame([], $calls);
    }

    public function test_mv_at_copy_part_size_is_one_copy_object(): void
    {
        $calls = [];

        aws_s3_filesystem(
            'bucket',
            S3ClientMother::copying(5 * 1024 * 1024, $calls),
            aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
        )->mv(path('aws-s3://src'), path('aws-s3://dst'));

        static::assertSame(['HeadObject src', 'CopyObject dst', 'DeleteObject src'], $calls);
    }

    public function test_mv_aborts_and_keeps_the_source_when_a_part_copy_fails(): void
    {
        $calls = [];

        try {
            aws_s3_filesystem(
                'bucket',
                S3ClientMother::copyingWithStatusOn(12 * 1024 * 1024, 'UploadPartCopy#2', 500, $calls),
                aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
            )->mv(path('aws-s3://src'), path('aws-s3://dst'));
            static::fail('Expected the failed part copy to fail the move');
        } catch (ServerException) {
        }

        static::assertSame('AbortMultipartUpload dst', end($calls));
        static::assertNotContains('DeleteObject src', $calls);
    }

    public function test_mv_keeps_the_source_when_copy_object_answers_error_xml(): void
    {
        $calls = [];

        try {
            aws_s3_filesystem(
                'bucket',
                S3ClientMother::copyingWithErrorXmlOn(1024, 'CopyObject', $calls),
                aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
            )->mv(path('aws-s3://src'), path('aws-s3://dst'));
            static::fail('Expected the error XML to fail the move');
        } catch (RuntimeException $e) {
            static::assertSame(
                'S3 CopyObject of "aws-s3://src" to "aws-s3://dst" returned no ETag, the copy failed and "aws-s3://src" is kept',
                $e->getMessage(),
            );
        }

        static::assertNotContains('DeleteObject src', $calls);
    }

    public function test_mv_aborts_when_a_part_copy_answers_error_xml(): void
    {
        $calls = [];

        try {
            aws_s3_filesystem(
                'bucket',
                S3ClientMother::copyingWithErrorXmlOn(12 * 1024 * 1024, 'UploadPartCopy', $calls),
                aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
            )->mv(path('aws-s3://src'), path('aws-s3://dst'));
            static::fail('Expected the error XML to fail the move');
        } catch (RuntimeException $e) {
            static::assertSame(
                'S3 UploadPartCopy of part 1 from "aws-s3://src" to "aws-s3://dst" returned no ETag, the upload is aborted',
                $e->getMessage(),
            );
        }

        static::assertContains('AbortMultipartUpload dst', $calls);
        static::assertNotContains('DeleteObject src', $calls);
    }

    public function test_mv_aborts_when_complete_answers_error_xml(): void
    {
        $calls = [];

        try {
            aws_s3_filesystem(
                'bucket',
                S3ClientMother::copyingWithErrorXmlOn(12 * 1024 * 1024, 'CompleteMultipartUpload', $calls),
                aws_s3_filesystem_options()->withCopyPartSize(5 * 1024 * 1024),
            )->mv(path('aws-s3://src'), path('aws-s3://dst'));
            static::fail('Expected the error XML to fail the move');
        } catch (RuntimeException $e) {
            static::assertSame(
                'S3 CompleteMultipartUpload of "aws-s3://dst" returned no ETag, the upload is aborted',
                $e->getMessage(),
            );
        }

        static::assertContains('AbortMultipartUpload dst', $calls);
        static::assertNotContains('DeleteObject src', $calls);
    }
}

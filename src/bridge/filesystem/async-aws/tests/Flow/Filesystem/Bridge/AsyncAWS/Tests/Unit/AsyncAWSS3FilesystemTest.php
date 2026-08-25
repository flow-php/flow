<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Exception\InvalidSchemeException;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
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
}

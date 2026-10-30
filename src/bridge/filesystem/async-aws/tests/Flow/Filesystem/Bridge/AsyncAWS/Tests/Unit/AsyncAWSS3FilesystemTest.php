<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Exception\InvalidSchemeException;
use Flow\Filesystem\Stream\BlockFactory;
use Flow\Types\Exception\InvalidArgumentException;
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
}

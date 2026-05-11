<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use AsyncAws\S3\S3Client;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AsyncAwsS3FilesystemFactory;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\HttpClient;

final class AsyncAwsS3FilesystemFactoryTest extends TestCase
{
    public function test_applies_block_size_option(): void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1', 'access_key_id' => 'k', 'access_key_secret' => 's'],
            'options' => ['block_size' => 6 * 1024 * 1024],
        ]);

        static::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
    }

    public function test_mode_a_builds_filesystem_from_resolved_client(): void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'my-bucket',
            'client' => new S3Client(['accessKeyId' => 'k', 'accessKeySecret' => 's', 'region' => 'us-east-1']),
        ]);

        static::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
        static::assertSame('aws-s3', $filesystem->mount()->protocol);
    }

    public function test_mode_b_accepts_resolved_http_client_and_logger(): void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => [
                'region' => 'us-east-1',
                'access_key_id' => 'k',
                'access_key_secret' => 's',
                'http_client' => HttpClient::create(),
                'logger' => new NullLogger(),
            ],
        ]);

        static::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
    }

    public function test_mode_b_builds_client_from_inline_config(): void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'my-bucket',
            'client' => [
                'region' => 'eu-west-1',
                'access_key_id' => 'key',
                'access_key_secret' => 'secret',
            ],
        ]);

        static::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
    }

    public function test_mount_protocol_is_propagated_to_filesystem(): void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory())->create('warehouse', [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1', 'access_key_id' => 'k', 'access_key_secret' => 's'],
        ]);

        static::assertSame('warehouse', $filesystem->mount()->protocol);
    }

    public function test_throws_on_invalid_block_size_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options.block_size` must be an integer');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'options' => ['block_size' => 'not-an-int'],
        ]);
    }

    public function test_throws_on_missing_bucket(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty `bucket`');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', ['client' => []]);
    }

    public function test_throws_on_unknown_client_keys(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client` contains unknown keys: [nope]');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => ['nope' => 'x'],
        ]);
    }

    public function test_throws_on_unknown_options_keys(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` contains unknown keys: [nope]');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'options' => ['nope' => 1],
        ]);
    }

    public function test_throws_on_unknown_top_level_keys(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('received unknown keys: [container]');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'container' => 'nope',
        ]);
    }

    public function test_throws_when_client_is_invalid_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client` must be an array or AsyncAws\\S3\\S3Client instance');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => new \stdClass(),
        ]);
    }

    public function test_throws_when_http_client_is_wrong_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.http_client_service_id` must reference a service implementing');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => [
                'region' => 'us-east-1',
                'http_client' => new \stdClass(),
            ],
        ]);
    }

    public function test_throws_when_logger_is_wrong_type(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.logger_service_id` must reference a service implementing');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => [
                'region' => 'us-east-1',
                'logger' => new \stdClass(),
            ],
        ]);
    }

    public function test_throws_when_no_client_supplied(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exactly one of');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', ['bucket' => 'b']);
    }

    public function test_throws_when_options_is_not_array(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` must be an array');

        (new AsyncAwsS3FilesystemFactory())->create('aws-s3', [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'options' => 'not-an-array',
        ]);
    }

    public function test_type_returns_aws_s3(): void
    {
        static::assertSame('aws_s3', (new AsyncAwsS3FilesystemFactory())->type());
    }
}

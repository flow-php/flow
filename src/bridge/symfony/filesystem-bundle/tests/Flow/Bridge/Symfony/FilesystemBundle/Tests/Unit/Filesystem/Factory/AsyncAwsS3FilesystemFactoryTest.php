<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use AsyncAws\S3\S3Client;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AsyncAwsS3FilesystemFactory;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Protocol;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpClient\HttpClient;

final class AsyncAwsS3FilesystemFactoryTest extends TestCase
{
    public function test_applies_block_size_option() : void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1', 'access_key_id' => 'k', 'access_key_secret' => 's'],
            'options' => ['block_size' => 6 * 1024 * 1024],
        ]);

        self::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
    }

    public function test_mode_a_builds_filesystem_from_client_service_id() : void
    {
        $container = new ContainerBuilder();
        $container->set('app.s3_client', new S3Client(['accessKeyId' => 'k', 'accessKeySecret' => 's', 'region' => 'us-east-1']));

        $filesystem = (new AsyncAwsS3FilesystemFactory($container))->create(new Protocol('aws-s3'), ['bucket' => 'my-bucket', 'client_service_id' => 'app.s3_client']);

        self::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
        self::assertSame('aws-s3', $filesystem->protocol()->name);
    }

    public function test_mode_b_builds_client_from_inline_config() : void
    {
        $filesystem = (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'my-bucket',
            'client' => [
                'region' => 'eu-west-1',
                'access_key_id' => 'key',
                'access_key_secret' => 'secret',
            ],
        ]);

        self::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
    }

    public function test_mode_b_resolves_http_client_and_logger_from_container() : void
    {
        $container = new ContainerBuilder();
        $container->set('app.http_client', HttpClient::create());
        $container->set('app.logger', new NullLogger());

        $filesystem = (new AsyncAwsS3FilesystemFactory($container))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => [
                'region' => 'us-east-1',
                'access_key_id' => 'k',
                'access_key_secret' => 's',
                'http_client_service_id' => 'app.http_client',
                'logger_service_id' => 'app.logger',
            ],
        ]);

        self::assertInstanceOf(AsyncAWSS3Filesystem::class, $filesystem);
    }

    public function test_protocol_returns_aws_s3() : void
    {
        self::assertSame('aws-s3', (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->protocol()->name);
    }

    public function test_throws_on_invalid_block_size_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options.block_size` must be an integer');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'options' => ['block_size' => 'not-an-int'],
        ]);
    }

    public function test_throws_on_missing_bucket() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty `bucket`');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), ['client' => []]);
    }

    public function test_throws_on_unknown_client_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client` contains unknown keys: [nope]');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => ['nope' => 'x'],
        ]);
    }

    public function test_throws_on_unknown_options_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` contains unknown keys: [nope]');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'options' => ['nope' => 1],
        ]);
    }

    public function test_throws_on_unknown_top_level_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('received unknown keys: [container]');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'container' => 'nope',
        ]);
    }

    public function test_throws_on_wrong_protocol() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "aws-s3" cannot create filesystem for protocol "file"');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('file'), ['bucket' => 'b', 'client' => []]);
    }

    public function test_throws_when_both_client_modes_supplied() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exactly one of `client_service_id` or `client`');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client_service_id' => 'some.service',
            'client' => ['region' => 'us-east-1'],
        ]);
    }

    public function test_throws_when_client_service_not_s3_client() : void
    {
        $container = new ContainerBuilder();
        $container->set('app.wrong', new \stdClass());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('is not an instance of');

        (new AsyncAwsS3FilesystemFactory($container))->create(new Protocol('aws-s3'), ['bucket' => 'b', 'client_service_id' => 'app.wrong']);
    }

    public function test_throws_when_no_client_mode_supplied() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exactly one of');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), ['bucket' => 'b']);
    }

    public function test_throws_when_options_is_not_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` must be an array');

        (new AsyncAwsS3FilesystemFactory(new ContainerBuilder()))->create(new Protocol('aws-s3'), [
            'bucket' => 'b',
            'client' => ['region' => 'us-east-1'],
            'options' => 'not-an-array',
        ]);
    }
}

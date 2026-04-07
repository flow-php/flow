<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AzureBlobFilesystemFactory;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Flow\Filesystem\Protocol;
use Nyholm\Psr7\Factory\Psr17Factory;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\DependencyInjection\ContainerBuilder;
use Symfony\Component\HttpClient\Psr18Client;

final class AzureBlobFilesystemFactoryTest extends TestCase
{
    public function test_applies_block_size_option() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')]],
            'options' => ['block_size' => 8 * 1024 * 1024, 'list_blob_max_results' => 100],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_mode_a_builds_filesystem_from_client_service_id() : void
    {
        $container = new ContainerBuilder();
        $container->set('app.blob_service', self::createStub(BlobServiceInterface::class));

        $filesystem = (new AzureBlobFilesystemFactory($container))->create(new Protocol('azure-blob'), [
            'container' => 'my-container',
            'client_service_id' => 'app.blob_service',
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
        self::assertSame('azure-blob', $filesystem->protocol()->name);
    }

    public function test_mode_b_builds_blob_service_from_inline_shared_key() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'my-container',
            'client' => [
                'account_name' => 'myaccount',
                'auth' => ['shared_key' => \base64_encode('secret-key')],
            ],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_mode_b_resolves_optional_http_and_factories_from_container() : void
    {
        $container = new ContainerBuilder();
        $container->set('app.http_client', new Psr18Client());
        $container->set('app.request_factory', new Psr17Factory());
        $container->set('app.stream_factory', new Psr17Factory());
        $container->set('app.logger', new NullLogger());

        $filesystem = (new AzureBlobFilesystemFactory($container))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => [
                'account_name' => 'a',
                'auth' => ['shared_key' => \base64_encode('k')],
                'http_client_service' => 'app.http_client',
                'request_factory_service' => 'app.request_factory',
                'stream_factory_service' => 'app.stream_factory',
                'logger_service_id' => 'app.logger',
            ],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_mode_b_uses_azurite_url_factory_when_host_set() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'my-container',
            'client' => [
                'account_name' => 'devstoreaccount1',
                'auth' => ['shared_key' => \base64_encode('devkey')],
                'url_factory' => ['host' => '127.0.0.1', 'port' => '10000', 'https' => false],
            ],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_protocol_returns_azure_blob() : void
    {
        self::assertSame('azure-blob', (new AzureBlobFilesystemFactory(new ContainerBuilder()))->protocol()->name);
    }

    public function test_throws_on_invalid_block_size_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('block_size');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => 'k']],
            'options' => ['block_size' => 'nope'],
        ]);
    }

    public function test_throws_on_invalid_list_blob_max_results_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('list_blob_max_results');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => 'k']],
            'options' => ['list_blob_max_results' => 'nope'],
        ]);
    }

    public function test_throws_on_missing_container() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty `container`');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), ['client' => []]);
    }

    public function test_throws_on_unknown_auth_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.auth` contains unknown keys: [sas_token]');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k'), 'sas_token' => 'x']],
        ]);
    }

    public function test_throws_on_unknown_client_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client` contains unknown keys: [nope]');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')], 'nope' => 1],
        ]);
    }

    public function test_throws_on_unknown_options_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` contains unknown keys: [nope]');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')]],
            'options' => ['nope' => 1],
        ]);
    }

    public function test_throws_on_unknown_top_level_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('received unknown keys: [bucket]');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')]],
            'bucket' => 'no',
        ]);
    }

    public function test_throws_on_wrong_protocol() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Filesystem factory for protocol "azure-blob" cannot create filesystem for protocol "file"');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('file'), ['container' => 'c', 'client' => []]);
    }

    public function test_throws_when_both_client_modes_supplied() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exactly one of');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client_service_id' => 'x',
            'client' => ['account_name' => 'a'],
        ]);
    }

    public function test_throws_when_client_service_not_blob_service() : void
    {
        $container = new ContainerBuilder();
        $container->set('app.wrong', new \stdClass());

        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('is not an instance of');

        (new AzureBlobFilesystemFactory($container))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client_service_id' => 'app.wrong',
        ]);
    }

    public function test_throws_when_mode_b_missing_account_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.account_name`');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['auth' => ['shared_key' => 'k']],
        ]);
    }

    public function test_throws_when_mode_b_missing_shared_key() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.auth.shared_key`');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a'],
        ]);
    }

    public function test_throws_when_no_client_mode_supplied() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exactly one of');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), ['container' => 'c']);
    }

    public function test_throws_when_options_is_not_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` must be an array');

        (new AzureBlobFilesystemFactory(new ContainerBuilder()))->create(new Protocol('azure-blob'), [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => 'k']],
            'options' => 'nope',
        ]);
    }
}

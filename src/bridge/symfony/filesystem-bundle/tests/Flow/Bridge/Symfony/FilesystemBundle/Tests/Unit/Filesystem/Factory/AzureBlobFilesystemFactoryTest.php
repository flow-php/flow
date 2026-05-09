<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Filesystem\Factory;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory\AzureBlobFilesystemFactory;
use Flow\Filesystem\Bridge\Azure\AzureBlobFilesystem;
use Nyholm\Psr7\Factory\Psr17Factory;
use PHPUnit\Framework\TestCase;
use Psr\Log\NullLogger;
use Symfony\Component\HttpClient\Psr18Client;

final class AzureBlobFilesystemFactoryTest extends TestCase
{
    public function test_applies_block_size_option() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')]],
            'options' => ['block_size' => 8 * 1024 * 1024, 'list_blob_max_results' => 100],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_mode_a_builds_filesystem_from_resolved_client() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'my-container',
            'client' => self::createStub(BlobServiceInterface::class),
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
        self::assertSame('azure-blob', $filesystem->mount()->protocol);
    }

    public function test_mode_b_accepts_resolved_http_and_factories() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => [
                'account_name' => 'a',
                'auth' => ['shared_key' => \base64_encode('k')],
                'http_client' => new Psr18Client(),
                'request_factory' => new Psr17Factory(),
                'stream_factory' => new Psr17Factory(),
                'logger' => new NullLogger(),
            ],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_mode_b_builds_blob_service_from_inline_shared_key() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'my-container',
            'client' => [
                'account_name' => 'myaccount',
                'auth' => ['shared_key' => \base64_encode('secret-key')],
            ],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_mode_b_uses_azurite_url_factory_when_host_set() : void
    {
        $filesystem = (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'my-container',
            'client' => [
                'account_name' => 'devstoreaccount1',
                'auth' => ['shared_key' => \base64_encode('devkey')],
                'url_factory' => ['host' => '127.0.0.1', 'port' => '10000', 'https' => false],
            ],
        ]);

        self::assertInstanceOf(AzureBlobFilesystem::class, $filesystem);
    }

    public function test_throws_on_invalid_block_size_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('block_size');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => 'k']],
            'options' => ['block_size' => 'nope'],
        ]);
    }

    public function test_throws_on_invalid_list_blob_max_results_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('list_blob_max_results');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => 'k']],
            'options' => ['list_blob_max_results' => 'nope'],
        ]);
    }

    public function test_throws_on_missing_container() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('non-empty `container`');

        (new AzureBlobFilesystemFactory())->create('azure-blob', ['client' => []]);
    }

    public function test_throws_on_unknown_auth_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.auth` contains unknown keys: [sas_token]');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k'), 'sas_token' => 'x']],
        ]);
    }

    public function test_throws_on_unknown_client_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client` contains unknown keys: [nope]');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')], 'nope' => 1],
        ]);
    }

    public function test_throws_on_unknown_options_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` contains unknown keys: [nope]');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')]],
            'options' => ['nope' => 1],
        ]);
    }

    public function test_throws_on_unknown_top_level_keys() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('received unknown keys: [bucket]');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => \base64_encode('k')]],
            'bucket' => 'no',
        ]);
    }

    public function test_throws_when_client_is_invalid_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client` must be an array or Flow\\Azure\\SDK\\BlobServiceInterface instance');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => new \stdClass(),
        ]);
    }

    public function test_throws_when_http_client_is_wrong_type() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.http_client_service` must reference a service implementing');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => [
                'account_name' => 'a',
                'auth' => ['shared_key' => \base64_encode('k')],
                'http_client' => new \stdClass(),
            ],
        ]);
    }

    public function test_throws_when_mode_b_missing_account_name() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.account_name`');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['auth' => ['shared_key' => 'k']],
        ]);
    }

    public function test_throws_when_mode_b_missing_shared_key() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`client.auth.shared_key`');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a'],
        ]);
    }

    public function test_throws_when_no_client_supplied() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('exactly one of');

        (new AzureBlobFilesystemFactory())->create('azure-blob', ['container' => 'c']);
    }

    public function test_throws_when_options_is_not_array() : void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('`options` must be an array');

        (new AzureBlobFilesystemFactory())->create('azure-blob', [
            'container' => 'c',
            'client' => ['account_name' => 'a', 'auth' => ['shared_key' => 'k']],
            'options' => 'nope',
        ]);
    }

    public function test_type_returns_azure_blob() : void
    {
        self::assertSame('azure_blob', (new AzureBlobFilesystemFactory())->type());
    }
}

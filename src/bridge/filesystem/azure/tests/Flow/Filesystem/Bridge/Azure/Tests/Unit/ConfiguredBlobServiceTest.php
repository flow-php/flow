<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Bridge\Azure\ConfiguredBlobService;
use Flow\Filesystem\Exception\InvalidArgumentException;
use PHPUnit\Framework\TestCase;

final class ConfiguredBlobServiceTest extends TestCase
{
    public function test_the_configuration_round_trips_with_the_default_azure_url(): void
    {
        $source = new ConfiguredBlobService('account', 'container', 'c2VjcmV0');

        static::assertSame(
            [
                'account_name' => 'account',
                'container' => 'container',
                'shared_key' => 'c2VjcmV0',
                'url' => ['azure' => ['host' => 'blob.core.windows.net']],
            ],
            $source->normalize(),
        );
        static::assertEquals($source, ConfiguredBlobService::fromArray($source->normalize()));
    }

    public function test_the_configuration_round_trips_with_an_azurite_url(): void
    {
        $source = ConfiguredBlobService::fromArray([
            'account_name' => 'account',
            'container' => 'container',
            'shared_key' => 'c2VjcmV0',
            'url' => ['azurite' => ['host' => '127.0.0.1', 'port' => '10000', 'https' => false]],
        ]);

        static::assertSame(
            ['azurite' => ['host' => '127.0.0.1', 'port' => '10000', 'https' => false]],
            $source->normalize()['url'],
        );
        static::assertEquals($source, ConfiguredBlobService::fromArray($source->normalize()));
    }

    public function test_the_service_is_built_once(): void
    {
        $source = new ConfiguredBlobService('account', 'container', 'c2VjcmV0');

        static::assertInstanceOf(BlobServiceInterface::class, $source->blobService());
        static::assertSame($source->blobService(), $source->blobService());
    }

    public function test_an_azurite_service_is_built(): void
    {
        static::assertInstanceOf(
            BlobServiceInterface::class,
            (new ConfiguredBlobService('account', 'container', 'c2VjcmV0', ['azurite' => [
                'host' => '127.0.0.1',
                'port' => '10000',
                'https' => false,
            ]]))->blobService(),
        );
    }

    public function test_an_unknown_key_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Azure blob service configuration holds unknown keys: token');

        ConfiguredBlobService::fromArray([
            'account_name' => 'a',
            'container' => 'c',
            'shared_key' => 'k',
            'token' => 'x',
        ]);
    }

    public function test_a_url_that_is_neither_azure_nor_azurite_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Azure blob service "url" must be {azure: {host?}} or {azurite: {host?, port?, https?}}',
        );

        ConfiguredBlobService::fromArray([
            'account_name' => 'a',
            'container' => 'c',
            'shared_key' => 'k',
            'url' => ['gcs' => []],
        ]);
    }

    public function test_an_empty_account_name_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Azure account name can not be empty');

        new ConfiguredBlobService('', 'c', 'k');
    }
}

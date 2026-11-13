<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Unit;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Bridge\Azure\ConfiguredBlobService;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;
use Psr\Http\Client\ClientExceptionInterface;

use function Flow\Types\DSL\type_string;
use function microtime;
use function parse_url;
use function stream_socket_get_name;
use function stream_socket_server;

use const PHP_URL_PORT;

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
                'http' => ['timeout' => 30, 'max_duration' => 300, 'max_retries' => 3],
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

    public function test_http_limits_round_trip(): void
    {
        static::assertSame(
            ['timeout' => 1, 'max_duration' => 2, 'max_retries' => 0],
            ConfiguredBlobService::fromArray([
                'account_name' => 'a',
                'container' => 'c',
                'shared_key' => 'k',
                'http' => ['timeout' => 1, 'max_duration' => 2, 'max_retries' => 0],
            ])->normalize()['http'],
        );
    }

    public function test_http_defaults_are_filled(): void
    {
        static::assertSame(
            ['timeout' => 30, 'max_duration' => 300, 'max_retries' => 3],
            ConfiguredBlobService::fromArray([
                'account_name' => 'a',
                'container' => 'c',
                'shared_key' => 'k',
            ])->normalize()['http'],
        );
    }

    public function test_timeout_ends_a_call_to_a_silent_server(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        static::assertNotFalse($server);
        $blobService = ConfiguredBlobService::fromArray([
            'account_name' => 'a',
            'container' => 'c',
            'shared_key' => 'c2VjcmV0',
            'url' => ['azurite' => [
                'host' => '127.0.0.1',
                'port' => (string) parse_url(
                    'tcp://' . type_string()->assert(stream_socket_get_name($server, false)),
                    PHP_URL_PORT,
                ),
                'https' => false,
            ]],
            'http' => ['timeout' => 0.2, 'max_duration' => 0, 'max_retries' => 0],
        ])->blobService();
        $start = microtime(true);

        try {
            $blobService->getBlobProperties('a.txt');
            static::fail('Expected the silent server to time out');
        } catch (ClientExceptionInterface) {
        }

        static::assertLessThan(1.0, microtime(true) - $start);
    }

    public function test_unknown_http_key_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'Azure blob service "http" holds unknown keys: timeouts. Allowed: timeout, max_duration, max_retries',
        );

        ConfiguredBlobService::fromArray([
            'account_name' => 'a',
            'container' => 'c',
            'shared_key' => 'k',
            'http' => ['timeouts' => 1],
        ]);
    }

    public static function invalid_http_limits(): Generator
    {
        yield 'zero timeout' => [['timeout' => 0], 'Azure blob service "http.timeout" must be a number above 0, got 0'];
        yield 'string timeout' => [
            ['timeout' => '1'],
            'Azure blob service "http.timeout" must be a number above 0, got \'1\'',
        ];
        yield 'negative max_duration' => [
            ['max_duration' => -1],
            'Azure blob service "http.max_duration" must be 0 (no limit) or a number above 0, got -1',
        ];
        yield 'string max_duration' => [
            ['max_duration' => '1'],
            'Azure blob service "http.max_duration" must be 0 (no limit) or a number above 0, got \'1\'',
        ];
        yield 'negative max_retries' => [
            ['max_retries' => -1],
            'Azure blob service "http.max_retries" must be an integer of 0 or more, got -1',
        ];
        yield 'float max_retries' => [
            ['max_retries' => 1.5],
            'Azure blob service "http.max_retries" must be an integer of 0 or more, got 1.5',
        ];
    }

    /**
     * @param array<string, mixed> $http
     */
    #[DataProvider('invalid_http_limits')]
    public function test_invalid_http_limit_is_rejected(array $http, string $message): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage($message);

        ConfiguredBlobService::fromArray([
            'account_name' => 'a',
            'container' => 'c',
            'shared_key' => 'k',
            'http' => $http,
        ]);
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\AsyncAWS\Tests\Unit;

use AsyncAws\Core\Exception\Http\NetworkException;
use AsyncAws\Core\Exception\InvalidArgument;
use AsyncAws\S3\S3Client;
use Flow\Filesystem\Bridge\AsyncAWS\AsyncAWSS3Filesystem;
use Flow\Filesystem\Bridge\AsyncAWS\ConfiguredS3Client;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Exception\InvalidArgumentException;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\type_string;
use function microtime;
use function stream_socket_get_name;
use function stream_socket_server;

final class ConfiguredS3ClientTest extends TestCase
{
    public function test_the_configuration_is_kept_verbatim(): void
    {
        $configuration = [
            'region' => 'eu-west-1',
            'accessKeyId' => 'k',
            'accessKeySecret' => 's',
            'endpoint' => 'http://s3.test',
        ];

        static::assertSame($configuration, (new ConfiguredS3Client($configuration))->normalize());
    }

    public function test_the_client_is_built_once(): void
    {
        $source = new ConfiguredS3Client(['region' => 'eu-west-1', 'accessKeyId' => 'k', 'accessKeySecret' => 's']);

        static::assertInstanceOf(S3Client::class, $source->client());
        static::assertSame($source->client(), $source->client());
    }

    public function test_normalize_keeps_the_http_limits(): void
    {
        $configuration = ['region' => 'x', 'http' => ['timeout' => 1, 'max_duration' => 2, 'max_retries' => 0]];
        $source = new ConfiguredS3Client($configuration);

        static::assertSame($configuration, $source->normalize());
        static::assertInstanceOf(S3Client::class, $source->client());
    }

    public function test_timeout_ends_a_call_to_a_silent_server(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        static::assertNotFalse($server);
        $client = (new ConfiguredS3Client([
            'region' => 'eu-west-1',
            'accessKeyId' => 'k',
            'accessKeySecret' => 's',
            'pathStyleEndpoint' => 'true',
            'endpoint' => 'http://' . type_string()->assert(stream_socket_get_name($server, false)),
            'http' => ['timeout' => 0.2, 'max_duration' => 0, 'max_retries' => 0],
        ]))->client();
        $start = microtime(true);

        try {
            $client->headObject(['Bucket' => 'bucket', 'Key' => 'a.txt'])->resolve();
            static::fail('Expected the silent server to time out');
        } catch (NetworkException) {
        }

        static::assertLessThan(1.0, microtime(true) - $start);
    }

    public function test_max_duration_bounds_a_call_with_its_retries(): void
    {
        $server = stream_socket_server('tcp://127.0.0.1:0');
        static::assertNotFalse($server);
        $client = (new ConfiguredS3Client([
            'region' => 'eu-west-1',
            'accessKeyId' => 'k',
            'accessKeySecret' => 's',
            'pathStyleEndpoint' => 'true',
            'endpoint' => 'http://' . type_string()->assert(stream_socket_get_name($server, false)),
            'http' => ['timeout' => 0.2, 'max_duration' => 0.5, 'max_retries' => 3],
        ]))->client();
        $start = microtime(true);

        try {
            $client->headObject(['Bucket' => 'bucket', 'Key' => 'a.txt'])->resolve();
            static::fail('Expected the silent server to time out');
        } catch (NetworkException) {
        }

        static::assertLessThan(1.5, microtime(true) - $start);
    }

    public function test_unknown_http_key_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'S3 client "http" holds unknown keys: timeouts. Allowed: timeout, max_duration, max_retries',
        );

        new ConfiguredS3Client(['region' => 'x', 'http' => ['timeouts' => 1]]);
    }

    public function test_an_unknown_client_option_is_reported_by_async_aws(): void
    {
        $this->expectException(InvalidArgument::class);
        $this->expectExceptionMessage('Invalid option(s) "regions"');

        AsyncAWSS3Filesystem::fromArray([
            'protocol' => 'aws-s3',
            'bucket' => 'bucket',
            'client' => ['regions' => ['eu-west-1'], 'http' => ['timeout' => 1]],
            'options' => (new Options())->normalize(),
        ])->readFrom(path('aws-s3://a.txt'));
    }

    public function test_http_that_is_not_an_array_is_rejected(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'S3 client "http" must be an array of timeout, max_duration and max_retries, got string',
        );

        new ConfiguredS3Client(['region' => 'x', 'http' => '30']);
    }

    public static function invalid_http_limits(): Generator
    {
        yield 'zero timeout' => [['timeout' => 0], 'S3 client "http.timeout" must be a number above 0, got 0'];
        yield 'string timeout' => [['timeout' => '1'], 'S3 client "http.timeout" must be a number above 0, got \'1\''];
        yield 'negative max_duration' => [
            ['max_duration' => -1],
            'S3 client "http.max_duration" must be 0 (no limit) or a number above 0, got -1',
        ];
        yield 'string max_duration' => [
            ['max_duration' => '1'],
            'S3 client "http.max_duration" must be 0 (no limit) or a number above 0, got \'1\'',
        ];
        yield 'negative max_retries' => [
            ['max_retries' => -1],
            'S3 client "http.max_retries" must be an integer of 0 or more, got -1',
        ];
        yield 'float max_retries' => [
            ['max_retries' => 1.5],
            'S3 client "http.max_retries" must be an integer of 0 or more, got 1.5',
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

        new ConfiguredS3Client(['region' => 'x', 'http' => $http]);
    }
}

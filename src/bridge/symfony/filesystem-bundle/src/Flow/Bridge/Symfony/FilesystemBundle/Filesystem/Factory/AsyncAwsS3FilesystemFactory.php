<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use AsyncAws\S3\S3Client;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Filesystem;
use Psr\Log\LoggerInterface;
use Symfony\Contracts\HttpClient\HttpClientInterface;

use function array_diff;
use function array_key_exists;
use function array_keys;
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_client;
use function Flow\Filesystem\Bridge\AsyncAWS\DSL\aws_s3_filesystem;
use function get_debug_type;
use function implode;
use function is_array;
use function is_int;
use function is_scalar;
use function is_string;
use function sprintf;

final readonly class AsyncAwsS3FilesystemFactory implements FilesystemFactory
{
    public function create(string $protocol, array $config): Filesystem
    {
        $allowed = ['bucket', 'client', 'options'];
        $unknown = array_diff(array_keys($config), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "aws_s3" received unknown keys: [%s]. Allowed: [%s].',
                implode(', ', $unknown),
                implode(', ', $allowed),
            ));
        }

        if (!is_string($config['bucket'] ?? null) || $config['bucket'] === '') {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "aws_s3" requires a non-empty `bucket` option.',
            );
        }

        $bucket = $config['bucket'];

        if (!array_key_exists('client', $config) || $config['client'] === null) {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "aws_s3" requires exactly one of `client_service_id` or `client`.',
            );
        }

        $client = $config['client'];

        if ($client instanceof S3Client) {
            $resolvedClient = $client;
        } elseif (is_array($client)) {
            /** @var array<string, mixed> $client */
            $resolvedClient = $this->buildClient($client);
        } else {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "aws_s3" `client` must be an array or %s instance, got %s.',
                S3Client::class,
                get_debug_type($client),
            ));
        }

        $options = $this->buildOptions($config['options'] ?? null);

        return aws_s3_filesystem($bucket, $resolvedClient, $options, $protocol);
    }

    public function type(): string
    {
        return 'aws_s3';
    }

    /**
     * @param array<string, mixed> $clientConfig
     */
    private function buildClient(array $clientConfig): S3Client
    {
        $allowed = [
            'region',
            'access_key_id',
            'access_key_secret',
            'session_token',
            'endpoint',
            'path_style_endpoint',
            'shared_credentials_file',
            'shared_config_file',
            'profile',
            'debug',
            'http_client',
            'logger',
        ];
        $unknown = array_diff(array_keys($clientConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "aws_s3" `client` contains unknown keys: [%s]. Allowed: [%s].',
                implode(', ', $unknown),
                implode(', ', $allowed),
            ));
        }

        $httpClient = null;
        $logger = null;

        if (array_key_exists('http_client', $clientConfig) && $clientConfig['http_client'] !== null) {
            if (!$clientConfig['http_client'] instanceof HttpClientInterface) {
                throw new InvalidArgumentException(sprintf(
                    'Filesystem factory for backend "aws_s3" `client.http_client_service_id` must reference a service implementing %s.',
                    HttpClientInterface::class,
                ));
            }
            $httpClient = $clientConfig['http_client'];
            unset($clientConfig['http_client']);
        }

        if (array_key_exists('logger', $clientConfig) && $clientConfig['logger'] !== null) {
            if (!$clientConfig['logger'] instanceof LoggerInterface) {
                throw new InvalidArgumentException(sprintf(
                    'Filesystem factory for backend "aws_s3" `client.logger_service_id` must reference a service implementing %s.',
                    LoggerInterface::class,
                ));
            }
            $logger = $clientConfig['logger'];
            unset($clientConfig['logger']);
        }

        $keyMap = [
            'region' => 'region',
            'access_key_id' => 'accessKeyId',
            'access_key_secret' => 'accessKeySecret',
            'session_token' => 'sessionToken',
            'endpoint' => 'endpoint',
            'path_style_endpoint' => 'pathStyleEndpoint',
            'shared_credentials_file' => 'sharedCredentialsFile',
            'shared_config_file' => 'sharedConfigFile',
            'profile' => 'profile',
            'debug' => 'debug',
        ];

        $asyncConfig = [];

        foreach ($keyMap as $from => $to) {
            if (array_key_exists($from, $clientConfig) && is_scalar($clientConfig[$from])) {
                $asyncConfig[$to] = (string) $clientConfig[$from];
            }
        }

        $client = aws_s3_client($asyncConfig);

        if ($httpClient !== null || $logger !== null) {
            return new S3Client($asyncConfig, null, $httpClient, $logger);
        }

        return $client;
    }

    private function buildOptions(mixed $optionsConfig): Options
    {
        $options = new Options();

        if ($optionsConfig === null) {
            return $options;
        }

        if (!is_array($optionsConfig)) {
            throw new InvalidArgumentException('Filesystem factory for backend "aws_s3" `options` must be an array.');
        }

        $allowed = ['block_size'];
        $unknown = array_diff(array_keys($optionsConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "aws_s3" `options` contains unknown keys: [%s]. Allowed: [%s].',
                implode(', ', $unknown),
                implode(', ', $allowed),
            ));
        }

        if (array_key_exists('block_size', $optionsConfig) && $optionsConfig['block_size'] !== null) {
            if (!is_int($optionsConfig['block_size'])) {
                throw new InvalidArgumentException('`options.block_size` must be an integer.');
            }
            $options = $options->withBlockSize($optionsConfig['block_size']);
        }

        return $options;
    }
}

<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use function Flow\Filesystem\Bridge\AsyncAWS\DSL\{aws_s3_client, aws_s3_filesystem};
use AsyncAws\S3\S3Client;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Bridge\AsyncAWS\Options;
use Flow\Filesystem\Filesystem;
use Psr\Container\ContainerInterface;

final readonly class AsyncAwsS3FilesystemFactory implements FilesystemFactory
{
    public function __construct(private ContainerInterface $container)
    {
    }

    public function create(string $protocol, array $config) : Filesystem
    {
        $allowed = ['bucket', 'client_service_id', 'client', 'options'];
        $unknown = \array_diff(\array_keys($config), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "aws_s3" received unknown keys: [%s]. Allowed: [%s].',
                \implode(', ', $unknown),
                \implode(', ', $allowed),
            ));
        }

        if (!\is_string($config['bucket'] ?? null) || $config['bucket'] === '') {
            throw new InvalidArgumentException('Filesystem factory for backend "aws_s3" requires a non-empty `bucket` option.');
        }

        $bucket = $config['bucket'];
        $clientServiceId = $config['client_service_id'] ?? null;
        $clientConfig = $config['client'] ?? null;

        if (($clientServiceId === null) === ($clientConfig === null)) {
            throw new InvalidArgumentException('Filesystem factory for backend "aws_s3" requires exactly one of `client_service_id` or `client`.');
        }

        if (\is_string($clientServiceId) && $clientServiceId !== '') {
            $client = $this->container->get($clientServiceId);

            if (!$client instanceof S3Client) {
                throw new InvalidArgumentException(\sprintf('Service "%s" is not an instance of %s.', $clientServiceId, S3Client::class));
            }
        } else {
            /** @var array<string, mixed> $clientConfig */
            $client = $this->buildClient($clientConfig ?? []);
        }

        $options = $this->buildOptions($config['options'] ?? null);

        return aws_s3_filesystem($bucket, $client, $options, $protocol);
    }

    public function type() : string
    {
        return 'aws_s3';
    }

    /**
     * @param array<string, mixed> $clientConfig
     */
    private function buildClient(array $clientConfig) : S3Client
    {
        $allowed = ['region', 'access_key_id', 'access_key_secret', 'session_token', 'endpoint', 'path_style_endpoint', 'shared_credentials_file', 'shared_config_file', 'profile', 'debug', 'http_client_service_id', 'logger_service_id'];
        $unknown = \array_diff(\array_keys($clientConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "aws_s3" `client` contains unknown keys: [%s]. Allowed: [%s].',
                \implode(', ', $unknown),
                \implode(', ', $allowed),
            ));
        }

        $httpClient = null;
        $logger = null;

        if (\array_key_exists('http_client_service_id', $clientConfig)) {
            $id = $clientConfig['http_client_service_id'];

            if (\is_string($id) && $id !== '') {
                $httpClient = $this->container->get($id);
            }
            unset($clientConfig['http_client_service_id']);
        }

        if (\array_key_exists('logger_service_id', $clientConfig)) {
            $id = $clientConfig['logger_service_id'];

            if (\is_string($id) && $id !== '') {
                $logger = $this->container->get($id);
            }
            unset($clientConfig['logger_service_id']);
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
            if (\array_key_exists($from, $clientConfig) && $clientConfig[$from] !== null) {
                $asyncConfig[$to] = $clientConfig[$from];
            }
        }

        $client = aws_s3_client($asyncConfig);

        if ($httpClient !== null || $logger !== null) {
            /** @phpstan-ignore-next-line */
            return new S3Client($asyncConfig, null, $httpClient, $logger);
        }

        return $client;
    }

    private function buildOptions(mixed $optionsConfig) : Options
    {
        $options = new Options();

        if ($optionsConfig === null) {
            return $options;
        }

        if (!\is_array($optionsConfig)) {
            throw new InvalidArgumentException('Filesystem factory for backend "aws_s3" `options` must be an array.');
        }

        $allowed = ['block_size'];
        $unknown = \array_diff(\array_keys($optionsConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "aws_s3" `options` contains unknown keys: [%s]. Allowed: [%s].',
                \implode(', ', $unknown),
                \implode(', ', $allowed),
            ));
        }

        if (\array_key_exists('block_size', $optionsConfig) && $optionsConfig['block_size'] !== null) {
            if (!\is_int($optionsConfig['block_size'])) {
                throw new InvalidArgumentException('`options.block_size` must be an integer.');
            }
            $options = $options->withBlockSize($optionsConfig['block_size']);
        }

        return $options;
    }
}

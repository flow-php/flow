<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Filesystem;
use Http\Discovery\Psr17FactoryDiscovery;
use Http\Discovery\Psr18ClientDiscovery;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Log\LoggerInterface;

use function array_diff;
use function array_key_exists;
use function array_keys;
use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_http_factory;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Azure\SDK\DSL\azure_url_factory;
use function Flow\Azure\SDK\DSL\azurite_url_factory;
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use function get_debug_type;
use function implode;
use function is_array;
use function is_int;
use function is_string;
use function sprintf;

final readonly class AzureBlobFilesystemFactory implements FilesystemFactory
{
    public function create(string $protocol, array $config): Filesystem
    {
        $allowed = ['container', 'client', 'options'];
        $unknown = array_diff(array_keys($config), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "azure_blob" received unknown keys: [%s]. Allowed: [%s].',
                implode(', ', $unknown),
                implode(', ', $allowed),
            ));
        }

        if (!is_string($config['container'] ?? null) || $config['container'] === '') {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "azure_blob" requires a non-empty `container` option.',
            );
        }

        $containerName = $config['container'];

        if (!array_key_exists('client', $config) || $config['client'] === null) {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "azure_blob" requires exactly one of `client_service_id` or `client`.',
            );
        }

        if ($config['client'] instanceof BlobServiceInterface) {
            $blobService = $config['client'];
        } elseif (is_array($config['client'])) {
            $blobService = $this->buildBlobService($containerName, $config['client']);
        } else {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "azure_blob" `client` must be an array or %s instance, got %s.',
                BlobServiceInterface::class,
                get_debug_type($config['client']),
            ));
        }

        $options = $this->buildOptions($config['options'] ?? null);

        return azure_filesystem($blobService, $options, $protocol);
    }

    public function type(): string
    {
        return 'azure_blob';
    }

    /**
     * @param array<array-key, mixed> $clientConfig
     */
    private function buildBlobService(string $containerName, array $clientConfig): BlobServiceInterface
    {
        $allowed = [
            'account_name',
            'auth',
            'url_factory',
            'http_client',
            'request_factory',
            'stream_factory',
            'logger',
        ];
        $unknown = array_diff(array_keys($clientConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "azure_blob" `client` contains unknown keys: [%s]. Allowed: [%s].',
                implode(', ', $unknown),
                implode(', ', $allowed),
            ));
        }

        if (!is_string($clientConfig['account_name'] ?? null) || $clientConfig['account_name'] === '') {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "azure_blob" `client.account_name` must be a non-empty string.',
            );
        }

        $accountName = $clientConfig['account_name'];

        if (
            !is_array($clientConfig['auth'] ?? null)
            || !is_string($clientConfig['auth']['shared_key'] ?? null)
            || $clientConfig['auth']['shared_key'] === ''
        ) {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "azure_blob" `client.auth.shared_key` must be a non-empty string.',
            );
        }

        $authAllowed = ['shared_key'];
        $authUnknown = array_diff(array_keys($clientConfig['auth']), $authAllowed);

        if ($authUnknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "azure_blob" `client.auth` contains unknown keys: [%s]. Allowed: [%s].',
                implode(', ', $authUnknown),
                implode(', ', $authAllowed),
            ));
        }

        $authFactory = azure_shared_key_authorization_factory($accountName, $clientConfig['auth']['shared_key']);
        $configuration = azure_blob_service_config($accountName, $containerName);

        $httpClient = $this->resolveResolvedService(
            $clientConfig['http_client'] ?? null,
            ClientInterface::class,
            'http_client_service',
        ) ?? Psr18ClientDiscovery::find();
        $requestFactory = $this->resolveResolvedService(
            $clientConfig['request_factory'] ?? null,
            RequestFactoryInterface::class,
            'request_factory_service',
        ) ?? Psr17FactoryDiscovery::findRequestFactory();
        $streamFactory = $this->resolveResolvedService(
            $clientConfig['stream_factory'] ?? null,
            StreamFactoryInterface::class,
            'stream_factory_service',
        ) ?? Psr17FactoryDiscovery::findStreamFactory();
        $logger = $this->resolveResolvedService(
            $clientConfig['logger'] ?? null,
            LoggerInterface::class,
            'logger_service_id',
        );

        $httpFactory = azure_http_factory($requestFactory, $streamFactory);

        // @mago-expect analysis:mixed-assignment
        $urlFactoryConfig = $clientConfig['url_factory'] ?? null;

        if (
            is_array($urlFactoryConfig)
            && is_string($urlFactoryConfig['host'] ?? null)
            && $urlFactoryConfig['host'] !== ''
        ) {
            $host = $urlFactoryConfig['host'];
            // @mago-expect analysis:mixed-assignment
            $portRaw = $urlFactoryConfig['port'] ?? '10000';
            $port = is_string($portRaw) ? $portRaw : (string) (is_int($portRaw) ? $portRaw : '10000');
            $https = ($urlFactoryConfig['https'] ?? false) === true;
            $urlFactory = azurite_url_factory($host, $port, $https);
        } else {
            $urlFactory = azure_url_factory();
        }

        return azure_blob_service($configuration, $authFactory, $httpClient, $httpFactory, $urlFactory, $logger);
    }

    private function buildOptions(mixed $optionsConfig): Options
    {
        $options = new Options();

        if ($optionsConfig === null) {
            return $options;
        }

        if (!is_array($optionsConfig)) {
            throw new InvalidArgumentException(
                'Filesystem factory for backend "azure_blob" `options` must be an array.',
            );
        }

        $allowed = ['block_size', 'list_blob_max_results'];
        $unknown = array_diff(array_keys($optionsConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "azure_blob" `options` contains unknown keys: [%s]. Allowed: [%s].',
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

        if (
            array_key_exists('list_blob_max_results', $optionsConfig)
            && $optionsConfig['list_blob_max_results'] !== null
        ) {
            if (!is_int($optionsConfig['list_blob_max_results'])) {
                throw new InvalidArgumentException('`options.list_blob_max_results` must be an integer.');
            }
            $options = $options->withListBlobMaxResults($optionsConfig['list_blob_max_results']);
        }

        return $options;
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $expectedClass
     *
     * @return null|T
     */
    private function resolveResolvedService(mixed $service, string $expectedClass, string $configKey): ?object
    {
        if ($service === null) {
            return null;
        }

        if (!$service instanceof $expectedClass) {
            throw new InvalidArgumentException(sprintf(
                'Filesystem factory for backend "azure_blob" `client.%s` must reference a service implementing %s.',
                $configKey,
                $expectedClass,
            ));
        }

        return $service;
    }
}

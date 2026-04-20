<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Filesystem\Factory;

use function Flow\Azure\SDK\DSL\{azure_blob_service, azure_blob_service_config, azure_http_factory, azure_shared_key_authorization_factory, azure_url_factory, azurite_url_factory};
use function Flow\Filesystem\Bridge\Azure\DSL\azure_filesystem;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Bridge\Symfony\FilesystemBundle\Exception\InvalidArgumentException;
use Flow\Bridge\Symfony\FilesystemBundle\Filesystem\FilesystemFactory;
use Flow\Filesystem\Bridge\Azure\Options;
use Flow\Filesystem\Filesystem;
use Http\Discovery\{Psr17FactoryDiscovery, Psr18ClientDiscovery};
use Psr\Container\ContainerInterface;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestFactoryInterface, StreamFactoryInterface};
use Psr\Log\LoggerInterface;

final readonly class AzureBlobFilesystemFactory implements FilesystemFactory
{
    public function __construct(private ContainerInterface $container)
    {
    }

    public function create(string $protocol, array $config) : Filesystem
    {
        $allowed = ['container', 'client_service_id', 'client', 'options'];
        $unknown = \array_diff(\array_keys($config), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "azure_blob" received unknown keys: [%s]. Allowed: [%s].',
                \implode(', ', $unknown),
                \implode(', ', $allowed),
            ));
        }

        if (!\is_string($config['container'] ?? null) || $config['container'] === '') {
            throw new InvalidArgumentException('Filesystem factory for backend "azure_blob" requires a non-empty `container` option.');
        }

        $container = $config['container'];
        $clientServiceId = $config['client_service_id'] ?? null;
        $clientConfig = $config['client'] ?? null;

        if (($clientServiceId === null) === ($clientConfig === null)) {
            throw new InvalidArgumentException('Filesystem factory for backend "azure_blob" requires exactly one of `client_service_id` or `client`.');
        }

        if (\is_string($clientServiceId) && $clientServiceId !== '') {
            $blobService = $this->container->get($clientServiceId);

            if (!$blobService instanceof BlobServiceInterface) {
                throw new InvalidArgumentException(\sprintf('Service "%s" is not an instance of %s.', $clientServiceId, BlobServiceInterface::class));
            }
        } else {
            /** @var array<string, mixed> $clientConfig */
            $blobService = $this->buildBlobService($container, $clientConfig ?? []);
        }

        $options = $this->buildOptions($config['options'] ?? null);

        return azure_filesystem($blobService, $options, $protocol);
    }

    public function type() : string
    {
        return 'azure_blob';
    }

    /**
     * @param array<string, mixed> $clientConfig
     */
    private function buildBlobService(string $containerName, array $clientConfig) : BlobServiceInterface
    {
        $allowed = ['account_name', 'auth', 'url_factory', 'http_client_service', 'request_factory_service', 'stream_factory_service', 'logger_service_id'];
        $unknown = \array_diff(\array_keys($clientConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "azure_blob" `client` contains unknown keys: [%s]. Allowed: [%s].',
                \implode(', ', $unknown),
                \implode(', ', $allowed),
            ));
        }

        if (!\is_string($clientConfig['account_name'] ?? null) || $clientConfig['account_name'] === '') {
            throw new InvalidArgumentException('Filesystem factory for backend "azure_blob" `client.account_name` must be a non-empty string.');
        }

        $accountName = $clientConfig['account_name'];

        if (!\is_array($clientConfig['auth'] ?? null) || !\is_string($clientConfig['auth']['shared_key'] ?? null) || $clientConfig['auth']['shared_key'] === '') {
            throw new InvalidArgumentException('Filesystem factory for backend "azure_blob" `client.auth.shared_key` must be a non-empty string.');
        }

        $authAllowed = ['shared_key'];
        $authUnknown = \array_diff(\array_keys($clientConfig['auth']), $authAllowed);

        if ($authUnknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "azure_blob" `client.auth` contains unknown keys: [%s]. Allowed: [%s].',
                \implode(', ', $authUnknown),
                \implode(', ', $authAllowed),
            ));
        }

        $authFactory = azure_shared_key_authorization_factory($accountName, $clientConfig['auth']['shared_key']);
        $configuration = azure_blob_service_config($accountName, $containerName);

        $httpClient = $this->resolveService($clientConfig['http_client_service'] ?? null, ClientInterface::class) ?? Psr18ClientDiscovery::find();
        $requestFactory = $this->resolveService($clientConfig['request_factory_service'] ?? null, RequestFactoryInterface::class) ?? Psr17FactoryDiscovery::findRequestFactory();
        $streamFactory = $this->resolveService($clientConfig['stream_factory_service'] ?? null, StreamFactoryInterface::class) ?? Psr17FactoryDiscovery::findStreamFactory();
        $logger = $this->resolveService($clientConfig['logger_service_id'] ?? null, LoggerInterface::class);

        $httpFactory = azure_http_factory($requestFactory, $streamFactory);

        $urlFactoryConfig = $clientConfig['url_factory'] ?? null;

        if (\is_array($urlFactoryConfig) && \is_string($urlFactoryConfig['host'] ?? null) && $urlFactoryConfig['host'] !== '') {
            $host = $urlFactoryConfig['host'];
            $portRaw = $urlFactoryConfig['port'] ?? '10000';
            $port = \is_string($portRaw) ? $portRaw : (string) (\is_int($portRaw) ? $portRaw : '10000');
            $https = (bool) ($urlFactoryConfig['https'] ?? false);
            $urlFactory = azurite_url_factory($host, $port, $https);
        } else {
            $urlFactory = azure_url_factory();
        }

        return azure_blob_service($configuration, $authFactory, $httpClient, $httpFactory, $urlFactory, $logger);
    }

    private function buildOptions(mixed $optionsConfig) : Options
    {
        $options = new Options();

        if ($optionsConfig === null) {
            return $options;
        }

        if (!\is_array($optionsConfig)) {
            throw new InvalidArgumentException('Filesystem factory for backend "azure_blob" `options` must be an array.');
        }

        $allowed = ['block_size', 'list_blob_max_results'];
        $unknown = \array_diff(\array_keys($optionsConfig), $allowed);

        if ($unknown !== []) {
            throw new InvalidArgumentException(\sprintf(
                'Filesystem factory for backend "azure_blob" `options` contains unknown keys: [%s]. Allowed: [%s].',
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

        if (\array_key_exists('list_blob_max_results', $optionsConfig) && $optionsConfig['list_blob_max_results'] !== null) {
            if (!\is_int($optionsConfig['list_blob_max_results'])) {
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
    private function resolveService(mixed $serviceId, string $expectedClass) : ?object
    {
        if (!\is_string($serviceId) || $serviceId === '') {
            return null;
        }

        $service = $this->container->get($serviceId);

        if (!$service instanceof $expectedClass) {
            throw new InvalidArgumentException(\sprintf('Service "%s" is not an instance of %s.', $serviceId, $expectedClass));
        }

        return $service;
    }
}

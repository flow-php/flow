<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure;

use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Filesystem\Exception\InvalidArgumentException;
use SensitiveParameter;

use function array_diff;
use function array_key_exists;
use function array_keys;
use function Flow\Azure\SDK\DSL\azure_blob_service;
use function Flow\Azure\SDK\DSL\azure_blob_service_config;
use function Flow\Azure\SDK\DSL\azure_shared_key_authorization_factory;
use function Flow\Azure\SDK\DSL\azure_url_factory;
use function Flow\Azure\SDK\DSL\azurite_url_factory;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_string;
use function implode;

/**
 * @type BlobServiceUrl = array{azure: array{host: string}}|array{azurite: array{host: string, port: string, https: bool}} canonical: every key filled
 * @type BlobServiceConfiguration = array{account_name: string, container: string, shared_key: string, url: BlobServiceUrl}
 */
final class ConfiguredBlobService implements BlobServiceSource
{
    private ?BlobServiceInterface $blobService = null;

    /**
     * @param BlobServiceUrl $url
     */
    public function __construct(
        private readonly string $accountName,
        private readonly string $container,
        #[SensitiveParameter]
        private readonly string $sharedKey,
        private readonly array $url = ['azure' => ['host' => 'blob.core.windows.net']],
    ) {
        if ($accountName === '') {
            throw new InvalidArgumentException('Azure account name can not be empty');
        }

        if ($container === '') {
            throw new InvalidArgumentException('Azure container can not be empty');
        }

        if ($sharedKey === '') {
            throw new InvalidArgumentException('Azure shared key can not be empty');
        }
    }

    /**
     * @param array<array-key, mixed> $config keys account_name, container, shared_key and optionally url
     *
     * @throws InvalidArgumentException
     */
    public static function fromArray(#[SensitiveParameter] array $config): self
    {
        $unknown = array_diff(array_keys($config), ['account_name', 'container', 'shared_key', 'url']);

        if ($unknown !== []) {
            throw new InvalidArgumentException(
                'Azure blob service configuration holds unknown keys: ' . implode(', ', $unknown),
            );
        }

        $url = type_array()->assert($config['url'] ?? ['azure' => []]);

        if (array_key_exists('azurite', $url)) {
            $azurite = type_array()->assert($url['azurite']);
            $url = ['azurite' => [
                'host' => type_string()->assert($azurite['host'] ?? 'localhost'),
                'port' => type_string()->assert($azurite['port'] ?? '10000'),
                'https' => type_boolean()->assert($azurite['https'] ?? false),
            ]];
        } elseif (array_key_exists('azure', $url)) {
            $azure = type_array()->assert($url['azure']);
            $url = ['azure' => ['host' => type_string()->assert($azure['host'] ?? 'blob.core.windows.net')]];
        } else {
            throw new InvalidArgumentException(
                'Azure blob service "url" must be {azure: {host?}} or {azurite: {host?, port?, https?}}',
            );
        }

        return new self(
            type_string()->assert($config['account_name'] ?? null),
            type_string()->assert($config['container'] ?? null),
            type_string()->assert($config['shared_key'] ?? null),
            $url,
        );
    }

    public function blobService(): BlobServiceInterface
    {
        if ($this->blobService !== null) {
            return $this->blobService;
        }

        $urlFactory = array_key_exists('azurite', $this->url)
            ? azurite_url_factory(
                $this->url['azurite']['host'],
                $this->url['azurite']['port'],
                $this->url['azurite']['https'],
            )
            : azure_url_factory($this->url['azure']['host']);

        return $this->blobService = azure_blob_service(
            azure_blob_service_config($this->accountName, $this->container),
            azure_shared_key_authorization_factory($this->accountName, $this->sharedKey),
            azure_url_factory: $urlFactory,
        );
    }

    /**
     * @return BlobServiceConfiguration
     */
    public function normalize(): array
    {
        return [
            'account_name' => $this->accountName,
            'container' => $this->container,
            'shared_key' => $this->sharedKey,
            'url' => $this->url,
        ];
    }
}

<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Azure\SDK\DSL\{azure_blob_service,
    azure_blob_service_config,
    azure_http_factory,
    azure_shared_key_authorization_factory,
    azurite_url_factory};
use function Flow\Types\DSL\type_string;
use Flow\Azure\SDK\{BlobServiceInterface, Exception\AzureException};
use Flow\ETL\Tests\FlowTestCase;
use Http\Discovery\{Psr17FactoryDiscovery, Psr18ClientDiscovery};

abstract class AzureBlobServiceTestCase extends FlowTestCase
{
    /**
     * @var array<string>
     */
    private array $containers = [];

    protected function tearDown() : void
    {
        foreach ($this->containers as $container) {
            try {
                $this->blobService($container)->deleteContainer();
            } catch (AzureException) {
            }
        }
    }

    public function givenFileExists(string $container, string $path, string $content) : void
    {
        $this->blobService($container)->putBlockBlob($path, $content, \strlen($content));
    }

    public function givenFileExistsFromPath(string $container, string $path, string $sourcePath) : void
    {
        $resource = fopen($sourcePath, 'rb');
        $filesize = \filesize($sourcePath);

        if ($resource === false) {
            throw new \RuntimeException('Unable to open file: ' . $sourcePath);
        }

        if ($filesize === false) {
            throw new \RuntimeException('Unable to get file size: ' . $sourcePath);
        }

        $this->blobService($container)->putBlockBlob($path, $resource, $filesize);
    }

    protected function blobService(string $container) : BlobServiceInterface
    {
        $accountName = type_string()->assert($_ENV['AZURITE_ACCOUNT_NAME']);
        $accountKey = type_string()->assert($_ENV['AZURITE_ACCOUNT_KEY']);
        $host = type_string()->assert($_ENV['AZURITE_HOST']);
        $port = type_string()->assert($_ENV['AZURITE_BLOB_PORT']);

        $blobService = azure_blob_service(
            azure_blob_service_config($accountName, $container),
            azure_shared_key_authorization_factory($accountName, $accountKey),
            Psr18ClientDiscovery::find(),
            azure_http_factory(Psr17FactoryDiscovery::findRequestFactory(), Psr17FactoryDiscovery::findStreamFactory()),
            azurite_url_factory($host, $port, false)
        );

        $properties = $blobService->getContainerProperties();

        if (!$properties) {
            $blobService->putContainer();
            $properties = $blobService->getContainerProperties();

            if (!\in_array($container, $this->containers, true)) {
                $this->containers[] = $container;
            }
        }

        return $blobService;
    }
}

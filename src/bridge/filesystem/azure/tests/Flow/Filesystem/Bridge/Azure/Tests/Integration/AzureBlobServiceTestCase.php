<?php

declare(strict_types=1);

namespace Flow\Filesystem\Bridge\Azure\Tests\Integration;

use function Flow\Azure\SDK\DSL\{azure_blob_service, azure_blob_service_config, azure_http_factory, azure_shared_key_authorization_factory, azurite_url_factory};
use Flow\Azure\SDK\{BlobService};
use Http\Discovery\{Psr17FactoryDiscovery, Psr18ClientDiscovery};
use PHPUnit\Framework\TestCase;

abstract class AzureBlobServiceTestCase extends TestCase
{
    /**
     * @var array<string>
     */
    private array $containers = [];

    protected function tearDown() : void
    {
        foreach ($this->containers as $container) {
            $this->blobService($container)->deleteContainer();
        }
    }

    public function givenFileExists(string $container, string $path, string $content) : void
    {
        $this->blobService($container)->putBlockBlob($path, $content, \strlen($content));
    }

    public function givenFileExistsFromPath(string $container, string $path, string $sourcePath) : void
    {
        $this->blobService($container)->putBlockBlob($path, fopen($sourcePath, 'rb'), \filesize($sourcePath));
    }

    protected function blobService(string $container) : BlobService
    {
        $blobService = azure_blob_service(
            azure_blob_service_config($_ENV['AZURITE_ACCOUNT_NAME'], $container),
            azure_shared_key_authorization_factory($_ENV['AZURITE_ACCOUNT_NAME'], $_ENV['AZURITE_ACCOUNT_KEY']),
            Psr18ClientDiscovery::find(),
            azure_http_factory(Psr17FactoryDiscovery::findRequestFactory(), Psr17FactoryDiscovery::findStreamFactory()),
            azurite_url_factory($_ENV['AZURITE_HOST'], $_ENV['AZURITE_BLOB_PORT'], false)
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

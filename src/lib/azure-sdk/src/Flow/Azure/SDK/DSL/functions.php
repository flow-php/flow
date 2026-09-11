<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\DSL;

use Flow\Azure\SDK\AuthorizationFactory;
use Flow\Azure\SDK\AuthorizationFactory\SharedKeyFactory;
use Flow\Azure\SDK\BlobService;
use Flow\Azure\SDK\BlobService\Configuration;
use Flow\Azure\SDK\BlobService\URLFactory\AzureURLFactory;
use Flow\Azure\SDK\BlobService\URLFactory\AzuriteURLFactory;
use Flow\Azure\SDK\BlobServiceInterface;
use Flow\Azure\SDK\HttpFactory;
use Flow\Azure\SDK\URLFactory;
use Flow\Documentation\Attribute\DocumentationDSL;
use Flow\Documentation\Attribute\Module;
use Flow\Documentation\Attribute\Type;
use Http\Discovery\Psr17FactoryDiscovery;
use Http\Discovery\Psr18ClientDiscovery;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestFactoryInterface;
use Psr\Http\Message\StreamFactoryInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use SensitiveParameter;

#[DocumentationDSL(module: Module::AZURE_SDK, type: Type::HELPER)]
function azurite_url_factory(
    string $host = 'localhost',
    string $port = '10000',
    bool $secure = false,
): AzuriteURLFactory {
    return new AzuriteURLFactory($host, $port, $secure);
}

#[DocumentationDSL(module: Module::AZURE_SDK, type: Type::HELPER)]
function azure_shared_key_authorization_factory(
    #[SensitiveParameter]
    string $account,
    #[SensitiveParameter]
    string $key,
): SharedKeyFactory {
    return new SharedKeyFactory($account, $key);
}

#[DocumentationDSL(module: Module::AZURE_SDK, type: Type::HELPER)]
function azure_blob_service_config(string $account, string $container): Configuration
{
    return new Configuration($account, $container);
}

#[DocumentationDSL(module: Module::AZURE_SDK, type: Type::HELPER)]
function azure_url_factory(string $host = 'blob.core.windows.net'): AzureURLFactory
{
    return new AzureURLFactory($host);
}

#[DocumentationDSL(module: Module::AZURE_SDK, type: Type::HELPER)]
function azure_http_factory(
    RequestFactoryInterface $request_factory,
    StreamFactoryInterface $stream_factory,
): HttpFactory {
    return new HttpFactory($request_factory, $stream_factory);
}

#[DocumentationDSL(module: Module::AZURE_SDK, type: Type::HELPER)]
function azure_blob_service(
    Configuration $configuration,
    AuthorizationFactory $azure_authorization_factory,
    ?ClientInterface $client = null,
    ?HttpFactory $azure_http_factory = null,
    ?URLFactory $azure_url_factory = null,
    ?LoggerInterface $logger = null,
): BlobServiceInterface {
    return new BlobService(
        $configuration,
        $client ?? Psr18ClientDiscovery::find(),
        $azure_http_factory ?? azure_http_factory(
            Psr17FactoryDiscovery::findRequestFactory(),
            Psr17FactoryDiscovery::findStreamFactory(),
        ),
        $azure_url_factory ?? azure_url_factory(),
        $azure_authorization_factory,
        $logger ?? new NullLogger(),
    );
}

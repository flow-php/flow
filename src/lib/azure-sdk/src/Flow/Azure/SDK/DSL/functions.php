<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\DSL;

use Flow\Azure\SDK\AuthorizationFactory\SharedKeyFactory;
use Flow\Azure\SDK\BlobService\Configuration;
use Flow\Azure\SDK\BlobService\URLFactory\{AzureURLFactory, AzuriteURLFactory};
use Flow\Azure\SDK\{AuthorizationFactory, BlobService, BlobServiceInterface, HttpFactory, URLFactory};
use Http\Discovery\{Psr17Factory, Psr18Client};
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestFactoryInterface, StreamFactoryInterface};
use Psr\Log\{LoggerInterface, NullLogger};

function azurite_url_factory(string $host, string $port, bool $secure) : AzuriteURLFactory
{
    return new AzuriteURLFactory($host, $port, $secure);
}

function azure_shared_key_authorization_factory(#[\SensitiveParameter] string $account, #[\SensitiveParameter] string $key) : SharedKeyFactory
{
    return new SharedKeyFactory($account, $key);
}

function azure_blob_service_config(string $account, string $container) : Configuration
{
    return new Configuration($account, $container);
}

function azure_url_factory(string $host = 'blob.core.windows.net') : AzureURLFactory
{
    return new AzureURLFactory($host);
}

function azure_http_factory(RequestFactoryInterface $request_factory, StreamFactoryInterface $stream_factory) : HttpFactory
{
    return new HttpFactory($request_factory, $stream_factory);
}

function azure_blob_service(
    Configuration $configuration,
    AuthorizationFactory $azure_authorization_factory,
    ClientInterface $client = new Psr18Client(),
    HttpFactory $azure_http_factory = new HttpFactory(new Psr17Factory(), new Psr17Factory()),
    URLFactory $azure_url_factory = new AzureURLFactory(),
    LoggerInterface $logger = new NullLogger()
) : BlobServiceInterface {
    return new BlobService($configuration, $client, $azure_http_factory, $azure_url_factory, $azure_authorization_factory, $logger);
}

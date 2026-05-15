<?php

declare(strict_types=1);

namespace Flow\Azure\SDK\Tests\Context;

use Flow\Azure\SDK\AuthorizationFactory\SharedKeyFactory;
use Flow\Azure\SDK\BlobService;
use Flow\Azure\SDK\BlobService\Configuration;
use Flow\Azure\SDK\BlobService\URLFactory\AzureURLFactory;
use Flow\Azure\SDK\HttpFactory;
use Flow\Azure\SDK\Tests\Double\FixedResponseClient;
use Http\Discovery\Psr17FactoryDiscovery;
use Psr\Http\Message\ResponseInterface;
use Psr\Log\NullLogger;

final class BlobServiceContext
{
    public static function service(ResponseInterface ...$responses): BlobService
    {
        return self::serviceWithClient(new FixedResponseClient(...$responses));
    }

    public static function serviceWithClient(FixedResponseClient $client): BlobService
    {
        return new BlobService(
            new Configuration('account', 'container'),
            $client,
            new HttpFactory(Psr17FactoryDiscovery::findRequestFactory(), Psr17FactoryDiscovery::findStreamFactory()),
            new AzureURLFactory(),
            new SharedKeyFactory('account', base64_encode('key')),
            new NullLogger(),
        );
    }

    public static function response(int $status, string $body): ResponseInterface
    {
        return Psr17FactoryDiscovery::findResponseFactory()
            ->createResponse($status)
            ->withBody(Psr17FactoryDiscovery::findStreamFactory()->createStream($body));
    }
}

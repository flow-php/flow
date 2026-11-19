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

    /**
     * @param array<int, int> $statuses status by Content-ID, in the order of the response parts
     */
    public static function batchResponse(array $statuses): ResponseInterface
    {
        $body = '';

        foreach ($statuses as $contentId => $status) {
            $body .=
                "--batchresponse_test\r\nContent-Type: application/http\r\nContent-ID: {$contentId}\r\n\r\n"
                . "HTTP/1.1 {$status} Status\r\n"
                . ($status >= 400 ? "x-ms-error-code: Code{$status}\r\n" : '')
                . "\r\n"
                . ($status >= 400 ? "<Error><Code>Code{$status}</Code></Error>\r\n" : '');
        }

        return self::responseWithHeaders(
            202,
            ['Content-Type' => 'multipart/mixed; boundary=batchresponse_test'],
            $body . "--batchresponse_test--\r\n",
        );
    }

    public static function response(int $status, string $body): ResponseInterface
    {
        return Psr17FactoryDiscovery::findResponseFactory()
            ->createResponse($status)
            ->withBody(Psr17FactoryDiscovery::findStreamFactory()->createStream($body));
    }

    /**
     * @param array<string, string> $headers
     */
    public static function responseWithHeaders(int $status, array $headers, string $body = ''): ResponseInterface
    {
        $response = self::response($status, $body);

        foreach ($headers as $name => $value) {
            $response = $response->withHeader($name, $value);
        }

        return $response;
    }
}

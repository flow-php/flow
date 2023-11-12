<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Double\Spy;

use Nyholm\Psr7\Response;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class HttpClientSpy implements ClientInterface
{
    public array $requests = [];

    public function sendRequest(RequestInterface $request) : ResponseInterface
    {
        $this->requests[] = $request;

        return new Response(
            200,
            [
                'Content-Type' => 'application/json',
            ],
            \json_encode([
                'taskUid' => 1000,
                'indexUid' => 'index-uid',
                'status' => 'succeeded',
            ])
        );
    }
}

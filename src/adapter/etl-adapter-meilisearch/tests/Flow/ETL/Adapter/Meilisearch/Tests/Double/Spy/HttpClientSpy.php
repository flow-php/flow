<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Double\Spy;

use Nyholm\Psr7\Response;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

final class HttpClientSpy implements ClientInterface
{
    /**
     * @var array<RequestInterface>
     */
    public array $requests = [];

    public function sendRequest(RequestInterface $request) : ResponseInterface
    {
        $this->requests[] = $request;

        $responseBody = \json_encode([
            'taskUid' => 1000,
            'indexUid' => 'index-uid',
            'status' => 'succeeded',
        ]);

        if ($responseBody === false) {
            throw new \RuntimeException('Failed to encode JSON response');
        }

        return new Response(
            200,
            [
                'Content-Type' => 'application/json',
            ],
            $responseBody
        );
    }
}

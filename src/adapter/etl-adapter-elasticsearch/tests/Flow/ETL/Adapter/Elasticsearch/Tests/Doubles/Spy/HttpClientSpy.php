<?php declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Doubles\Spy;

use Elastic\Elasticsearch\Response\Elasticsearch;
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
                Elasticsearch::HEADER_CHECK => Elasticsearch::PRODUCT_NAME,
            ]
        );
    }
}

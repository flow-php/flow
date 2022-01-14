<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * @psalm-immutable
 */
final class PsrHttpClientStaticExtractor implements Extractor
{
    private ClientInterface $client;

    /**
     * @var iterable<RequestInterface>
     */
    private iterable $requests;

    /**
     * @psalm-var pure-callable(RequestInterface) : void|null
     *
     * @var callable(RequestInterface) : void|null
     */
    private $preRequest;

    /**
     * @psalm-var pure-callable(RequestInterface, ResponseInterface) : void|null
     *
     * @var callable(RequestInterface, ResponseInterface) : void|null
     */
    private $postRequest;

    /**
     * @param ClientInterface $client
     * @param iterable<RequestInterface> $requests
     * @psalm-param pure-callable(RequestInterface) : void|null $preRequest
     * @psalm-param pure-callable(RequestInterface, ResponseInterface) : void|null $postRequest
     */
    public function __construct(ClientInterface $client, iterable $requests, ?callable $preRequest = null, ?callable $postRequest = null)
    {
        $this->client = $client;
        $this->requests = $requests;
        $this->preRequest = $preRequest;
        $this->postRequest = $postRequest;
    }

    public function extract() : \Generator
    {
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        foreach ($this->requests as $request) {
            if ($this->preRequest) {
                ($this->preRequest)($request);
            }

            /** @psalm-suppress ImpureMethodCall */
            $response = $this->client->sendRequest($request);

            if ($this->postRequest) {
                ($this->postRequest)($request, $response);
            }

            yield new Rows(
                Row::create(...\array_merge($responseFactory->create($response)->all(), $requestFactory->create($request)->all()))
            );
        }
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

/**
 * @psalm-immutable
 */
final class PsrHttpClientDynamicExtractor implements Extractor
{
    private ClientInterface $client;

    private NextRequestFactory $requestFactory;

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
     * @param NextRequestFactory $requestFactory
     * @psalm-param pure-callable(RequestInterface) : void|null $preRequest
     * @psalm-param pure-callable(RequestInterface, ResponseInterface) : void|null $postRequest
     */
    public function __construct(ClientInterface $client, NextRequestFactory $requestFactory, ?callable $preRequest = null, ?callable $postRequest = null)
    {
        $this->client = $client;
        $this->requestFactory = $requestFactory;
        $this->preRequest = $preRequest;
        $this->postRequest = $postRequest;
    }

    public function extract() : \Generator
    {
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        $nextRequest = $this->requestFactory->create();

        while ($nextRequest) {
            if ($this->preRequest) {
                ($this->preRequest)($nextRequest);
            }

            /** @psalm-suppress ImpureMethodCall */
            $response = $this->client->sendRequest($nextRequest);

            if ($this->postRequest) {
                ($this->postRequest)($nextRequest, $response);
            }

            yield new Rows(
                Row::create(...\array_merge($responseFactory->create($response)->all(), $requestFactory->create($nextRequest)->all()))
            );

            $nextRequest = $this->requestFactory->create($response);
        }
    }
}

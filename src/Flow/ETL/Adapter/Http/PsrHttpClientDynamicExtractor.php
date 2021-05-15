<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Extractor;
use Psr\Http\Client\ClientInterface;

/**
 * @psalm-immutable
 */
final class PsrHttpClientDynamicExtractor implements Extractor
{
    private ClientInterface $client;

    private NextRequestFactory $requestFactory;

    public function __construct(ClientInterface $client, NextRequestFactory $requestFactory)
    {
        $this->client = $client;
        $this->requestFactory = $requestFactory;
    }

    public function extract() : \Generator
    {
        $factory = new RowsResponseFactory();

        $nextRequest = $this->requestFactory->create();

        while ($nextRequest) {
            /** @psalm-suppress ImpureMethodCall */
            $response = $this->client->sendRequest($nextRequest);

            yield $factory->create($response);

            $nextRequest = $this->requestFactory->create($response);
        }
    }
}

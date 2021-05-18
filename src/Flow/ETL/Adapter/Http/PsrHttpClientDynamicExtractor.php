<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
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
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        $nextRequest = $this->requestFactory->create();

        while ($nextRequest) {
            /** @psalm-suppress ImpureMethodCall */
            $response = $this->client->sendRequest($nextRequest);

            yield new Rows(
                Row::create(...\array_merge($responseFactory->create($response)->all(), $requestFactory->create($nextRequest)->all()))
            );

            $nextRequest = $this->requestFactory->create($response);
        }
    }
}

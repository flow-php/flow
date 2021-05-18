<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Extractor;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;

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
     * @param ClientInterface $client
     * @param iterable<RequestInterface> $requests
     */
    public function __construct(ClientInterface $client, iterable $requests)
    {
        $this->client = $client;
        $this->requests = $requests;
    }

    public function extract() : \Generator
    {
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        foreach ($this->requests as $request) {
            /** @psalm-suppress ImpureMethodCall */
            $response = $this->client->sendRequest($request);

            yield new Rows(
                Row::create(...\array_merge($responseFactory->create($response)->all(), $requestFactory->create($request)->all()))
            );
        }
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\DSL\Entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class PsrHttpClientDynamicExtractor implements Extractor
{
    /**
     * @psalm-var callable(RequestInterface, ResponseInterface) : void|null
     *
     * @var null|callable(RequestInterface, ResponseInterface) : void
     */
    private $postRequest;

    /**
     * @psalm-var callable(RequestInterface) : void|null
     *
     * @var null|callable(RequestInterface) : void
     */
    private $preRequest;

    /**
     * @psalm-param callable(RequestInterface) : void|null $preRequest
     * @psalm-param callable(RequestInterface, ResponseInterface) : void|null $postRequest
     */
    public function __construct(private readonly ClientInterface $client, private readonly NextRequestFactory $requestFactory, ?callable $preRequest = null, ?callable $postRequest = null)
    {
        $this->preRequest = $preRequest;
        $this->postRequest = $postRequest;
    }

    public function extract(FlowContext $context) : \Generator
    {
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        $nextRequest = $this->requestFactory->create();

        while ($nextRequest) {
            if ($this->preRequest) {
                ($this->preRequest)($nextRequest);
            }

            $response = $this->client->sendRequest($nextRequest);

            if ($this->postRequest) {
                ($this->postRequest)($nextRequest, $response);
            }

            if ($context->config->shouldPutInputIntoRows()) {
                yield new Rows(
                    Row::create(
                        ...\array_merge(
                            $responseFactory->create($response)->all(),
                            $requestFactory->create($nextRequest)->all(),
                            [
                                Entry::string('request_uri', (string) $nextRequest->getUri()),
                                Entry::string('request_method', $nextRequest->getMethod()),
                                Entry::array('request_headers', $nextRequest->getHeaders()),
                            ]
                        )
                    )
                );
            } else {
                yield new Rows(
                    Row::create(...\array_merge($responseFactory->create($response)->all(), $requestFactory->create($nextRequest)->all()))
                );
            }

            $nextRequest = $this->requestFactory->create($response);
        }
    }
}

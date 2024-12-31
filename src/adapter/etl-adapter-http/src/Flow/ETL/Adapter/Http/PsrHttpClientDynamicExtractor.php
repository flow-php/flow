<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\{json_entry, str_entry};
use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\{Extractor, FlowContext, Row, Rows};
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

final class PsrHttpClientDynamicExtractor implements Extractor
{
    /**
     * @var null|callable(RequestInterface, ResponseInterface) : void
     */
    private $postRequest;

    /**
     * @var null|callable(RequestInterface) : void
     */
    private $preRequest;

    public function __construct(
        private readonly ClientInterface $client,
        private readonly NextRequestFactory $requestFactory,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        $nextRequest = $this->requestFactory->create();

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        while ($nextRequest) {
            if ($this->preRequest) {
                ($this->preRequest)($nextRequest);
            }

            $response = $this->client->sendRequest($nextRequest);

            if ($this->postRequest) {
                ($this->postRequest)($nextRequest, $response);
            }

            if ($shouldPutInputIntoRows) {
                $signal = yield new Rows(
                    Row::create(
                        ...\array_merge(
                            $responseFactory->create($response)->all(),
                            $requestFactory->create($nextRequest)->all(),
                            [
                                str_entry('request_uri', (string) $nextRequest->getUri()),
                                str_entry('request_method', $nextRequest->getMethod()),
                                json_entry('request_headers', $nextRequest->getHeaders()),
                            ]
                        )
                    )
                );

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }
            } else {
                $signal = yield new Rows(
                    Row::create(...\array_merge($responseFactory->create($response)->all(), $requestFactory->create($nextRequest)->all()))
                );

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }
            }

            $nextRequest = $this->requestFactory->create($response);
        }
    }

    /**
     * @param callable(RequestInterface, ResponseInterface) : void $postRequest
     */
    public function withPostRequest(callable $postRequest) : self
    {
        $this->postRequest = $postRequest;

        return $this;
    }

    /**
     * @param callable(RequestInterface) : void $preRequest
     */
    public function withPreRequest(callable $preRequest) : self
    {
        $this->preRequest = $preRequest;

        return $this;
    }
}

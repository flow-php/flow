<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\{array_entry, str_entry};
use Flow\ETL\{Extractor, FlowContext, Row, Rows};
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\{RequestInterface, ResponseInterface};

final class PsrHttpClientStaticExtractor implements Extractor
{
    /**
     * @var null|callable(RequestInterface, ResponseInterface) : void
     */
    private $postRequest;

    /**
     * @var null|callable(RequestInterface) : void
     */
    private $preRequest;

    /**
     * @param iterable<RequestInterface> $requests
     */
    public function __construct(
        private readonly ClientInterface $client,
        private readonly iterable $requests,
    ) {
    }

    public function extract(FlowContext $context) : \Generator
    {
        $responseFactory = new ResponseEntriesFactory();
        $requestFactory = new RequestEntriesFactory();

        $shouldPutInputIntoRows = $context->config->shouldPutInputIntoRows();

        foreach ($this->requests as $request) {
            if ($this->preRequest) {
                ($this->preRequest)($request);
            }

            $response = $this->client->sendRequest($request);

            if ($this->postRequest) {
                ($this->postRequest)($request, $response);
            }

            if ($shouldPutInputIntoRows) {
                $signal = yield new Rows(
                    Row::create(
                        ...\array_merge(
                            $responseFactory->create($response)->all(),
                            $requestFactory->create($request)->all(),
                            [
                                str_entry('request_uri', (string) $request->getUri()),
                                str_entry('request_method', $request->getMethod()),
                                array_entry('request_headers', $request->getHeaders()),
                            ]
                        )
                    )
                );

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }
            } else {
                $signal = yield new Rows(
                    Row::create(...\array_merge(
                        $responseFactory->create($response)->all(),
                        $requestFactory->create($request)->all()
                    ))
                );

                if ($signal === Extractor\Signal::STOP) {
                    return;
                }
            }
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

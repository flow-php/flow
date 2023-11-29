<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use function Flow\ETL\DSL\array_entry;
use function Flow\ETL\DSL\str_entry;
use Flow\ETL\Extractor;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

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
     * @param null|callable(RequestInterface) : void $preRequest
     * @param null|callable(RequestInterface, ResponseInterface) : void $postRequest
     */
    public function __construct(private readonly ClientInterface $client, private readonly iterable $requests, ?callable $preRequest = null, ?callable $postRequest = null)
    {
        $this->preRequest = $preRequest;
        $this->postRequest = $postRequest;
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
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Adapter\Http\Pagination\DecodedResponse;
use Flow\ETL\Adapter\Http\Pagination\Paginator;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use function is_array;

final class PsrHttpClientPaginatedExtractor implements Extractor
{
    /**
     * @var null|callable(RequestInterface, ResponseInterface) : void
     */
    private $postRequest;

    /**
     * @var null|callable(RequestInterface) : void
     */
    private $preRequest;

    private ?Schema $schema = null;

    public function __construct(
        private readonly ClientInterface $client,
        private readonly RequestInterface $request,
        private readonly Paginator $paginator,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $encoder = new HttpEncoder();
        $hydrator = $context->hydrator();

        $request = $this->paginator->initialRequest($this->request);

        while ($request !== null) {
            if ($this->preRequest) {
                ($this->preRequest)($request);
            }

            $response = $this->client->sendRequest($request);

            if ($this->postRequest) {
                ($this->postRequest)($request, $response);
            }

            $raw = $encoder->decode([new HttpExchange($request, $response)]);

            foreach ($hydrator->cast($raw, $this->schema) as $row) {
                $signal = yield new Rows($row);

                if ($signal === Signal::STOP) {
                    return;
                }
            }

            /** @var array<mixed>|string|null $body */
            $body = $raw[0]->values['response_body'];

            $request = $this->paginator->nextRequest(
                $this->request,
                new DecodedResponse(is_array($body) ? $body : [], $response),
            );
        }
    }

    /**
     * @param callable(RequestInterface, ResponseInterface) : void $postRequest
     */
    public function withPostRequest(callable $postRequest): self
    {
        $this->postRequest = $postRequest;

        return $this;
    }

    /**
     * @param callable(RequestInterface) : void $preRequest
     */
    public function withPreRequest(callable $preRequest): self
    {
        $this->preRequest = $preRequest;

        return $this;
    }

    public function withSchema(Schema $schema): self
    {
        $this->schema = clone $schema;

        return $this;
    }
}

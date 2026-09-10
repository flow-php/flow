<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Flow\ETL\Extractor;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Generator;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

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

    private ?Schema $schema = null;

    public function __construct(
        private readonly ClientInterface $client,
        private readonly NextRequestFactory $requestFactory,
    ) {}

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context): Generator
    {
        $encoder = new HttpEncoder();
        $hydrator = $context->hydrator();

        $nextRequest = $this->requestFactory->create();

        while ($nextRequest) {
            if ($this->preRequest) {
                ($this->preRequest)($nextRequest);
            }

            $response = $this->client->sendRequest($nextRequest);

            if ($this->postRequest) {
                ($this->postRequest)($nextRequest, $response);
            }

            $hydrated = $hydrator->hydrate($encoder->decode([new HttpExchange(
                $nextRequest,
                $response,
            )]), $this->schema());

            $signal = yield $hydrated;

            if ($signal === Signal::STOP) {
                return;
            }

            $nextRequest = $this->requestFactory->create($response);
        }
    }

    public function schema(): Schema
    {
        if ($this->schema !== null) {
            return $this->schema;
        }

        $headers = type_map(type_string(), type_list(type_string()));

        return schema(
            str_schema('response_body', nullable: true),
            map_schema('response_headers', $headers),
            int_schema('response_status_code'),
            str_schema('response_protocol_version'),
            str_schema('response_reason_phrase'),
            str_schema('request_body', nullable: true),
            str_schema('request_uri'),
            map_schema('request_headers', $headers),
            str_schema('request_protocol_version'),
            str_schema('request_method'),
        );
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

    public function withSchema(Schema $schema): static
    {
        $this->schema = $schema;

        return $this;
    }
}

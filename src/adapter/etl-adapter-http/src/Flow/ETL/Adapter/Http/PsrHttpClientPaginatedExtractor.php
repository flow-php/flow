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

use function Flow\ETL\DSL\int_schema;
use function Flow\ETL\DSL\map_schema;
use function Flow\ETL\DSL\schema;
use function Flow\ETL\DSL\str_schema;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_string;

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

            $hydrated = $hydrator->hydrate($raw, $this->schema());

            foreach ($hydrated as $row) {
                $signal = yield Rows::trusted($hydrated->schema(), [$row]);

                if ($signal === Signal::STOP) {
                    return;
                }
            }

            $request = $this->paginator->nextRequest(
                $this->request,
                new DecodedResponse($encoder->structuredBody($response), $response),
            );
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

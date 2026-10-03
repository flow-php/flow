<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http;

use Flow\ETL\Extractor;
use Flow\ETL\Extractor\BatchableExtractor;
use Flow\ETL\Extractor\Batches;
use Flow\ETL\Extractor\Signal;
use Flow\ETL\Extractor\Statistics;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\ETL\Rows\RowsBuilder;
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

final class PsrHttpClientStaticExtractor implements BatchableExtractor, Extractor
{
    use Batches;

    /**
     * @var null|callable(RequestInterface, ResponseInterface) : void
     */
    private $postRequest;

    /**
     * @var null|callable(RequestInterface) : void
     */
    private $preRequest;

    private ?Schema $schema = null;

    /**
     * @param iterable<RequestInterface> $requests
     */
    public function __construct(
        private readonly ClientInterface $client,
        private readonly iterable $requests,
    ) {
        // one request is one row: each response reaches the pipeline on its own unless withBatchSize() groups them
        $this->batchSize = 1;
    }

    /**
     * @return Generator<int, Rows, Signal|null, void>
     */
    public function extract(FlowContext $context, ?int $limit = null): Generator
    {
        $decoder = new HttpDecoder();
        $backend = $context->backend();
        $schema = $this->schema();
        $builder = new RowsBuilder($schema, $backend);
        $remaining = $limit;

        foreach ($this->requests as $request) {
            if ($this->preRequest) {
                ($this->preRequest)($request);
            }

            $response = $this->client->sendRequest($request);

            if ($this->postRequest) {
                ($this->postRequest)($request, $response);
            }

            $builder->appendRows($decoder->decode([new HttpExchange($request, $response)]));

            if ($builder->count() < $this->batchSize() && ($remaining === null || $builder->count() < $remaining)) {
                continue;
            }

            $batch = $builder->finish();
            $remaining = $remaining === null ? null : $remaining - $batch->count();
            $signal = yield $batch;

            if ($signal === Signal::STOP || $remaining === 0) {
                return;
            }

            $builder = new RowsBuilder($schema, $backend);
        }

        if ($builder->count() > 0) {
            yield $builder->finish();
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

    public function statistics(): Statistics
    {
        return new Statistics();
    }
}

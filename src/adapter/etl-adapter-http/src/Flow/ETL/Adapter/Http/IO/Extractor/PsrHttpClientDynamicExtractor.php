<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Http\IO\Extractor;

use Flow\ETL\IO\Extractor;
use Flow\ETL\IO\Extractor\Criteria;
use Flow\ETL\IO\Extractor\Signal;
use Flow\ETL\IO\Extractor\Statistics;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\Rows\RowsBuilder;
use Flow\ETL\Values\Schema;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Definition\MapDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Generator;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

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
    public function extract(Backend $backend, Criteria $criteria): Generator
    {
        $decoder = new HttpDecoder();

        $nextRequest = $this->requestFactory->create();

        while ($nextRequest) {
            if ($this->preRequest) {
                ($this->preRequest)($nextRequest);
            }

            $response = $this->client->sendRequest($nextRequest);

            if ($this->postRequest) {
                ($this->postRequest)($nextRequest, $response);
            }

            $signal = yield (new RowsBuilder($this->schema(), $backend))
                ->appendRows($decoder->decode([new HttpExchange($nextRequest, $response)]))
                ->finish();

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

        return new Schema(
            new StringDefinition('response_body', nullable: true),
            new MapDefinition('response_headers', $headers),
            new IntegerDefinition('response_status_code'),
            new StringDefinition('response_protocol_version'),
            new StringDefinition('response_reason_phrase'),
            new StringDefinition('request_body', nullable: true),
            new StringDefinition('request_uri'),
            new MapDefinition('request_headers', $headers),
            new StringDefinition('request_protocol_version'),
            new StringDefinition('request_method'),
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

    public function isRepeatable(): bool
    {
        return false;
    }
}

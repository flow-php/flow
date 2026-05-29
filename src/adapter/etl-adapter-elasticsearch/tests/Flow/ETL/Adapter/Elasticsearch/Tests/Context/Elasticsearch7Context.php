<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Context;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Elasticsearch\Common\Exceptions\BadRequest400Exception;
use Elasticsearch\Common\Exceptions\Missing404Exception;
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Rows;

use function Flow\ETL\Adapter\Elasticsearch\to_es_bulk_index;
use function Flow\ETL\DSL\config;
use function Flow\ETL\DSL\flow_context;

final class Elasticsearch7Context implements ElasticsearchContext
{
    // @mago-ignore analysis:non-existent-class-like
    private ?Client $client = null;

    /**
     * @param array<string> $hosts
     */
    public function __construct(
        private readonly array $hosts,
    ) {}

    // @mago-ignore analysis:non-existent-class-like
    public function client(): Client
    {
        if ($this->client === null) {
            // @mago-ignore analysis:non-existent-method
            // @mago-ignore analysis:mixed-property-type-coercion
            $this->client = ClientBuilder::fromConfig($this->clientConfig());
        }

        /**
         * @var Client $this->client
         */
        return $this->client;
    }

    /**
     * @return array{hosts?: array<string>, connectionParams?: array<mixed>, retries?: int, sniffOnStart?: bool, sslCert?: array<string>, sslKey?: array<string>, sslVerification?: bool|string, elasticMetaHeader?: bool, includePortInHostHeader?: bool}
     */
    public function clientConfig(): array
    {
        return [
            'hosts' => $this->hosts,
        ];
    }

    public function createIndex(string $name): void
    {
        try {
            $params = [
                'index' => $name,
                'body' => [
                    'settings' => [
                        'number_of_shards' => 2,
                        'number_of_replicas' => 0,
                    ],
                ],
            ];

            // @mago-ignore analysis:invalid-method-access
            // @mago-ignore analysis:mixed-method-access
            $this->client()->indices()->create($params);

            // @mago-ignore analysis:non-existent-catch-type
            // @mago-ignore analysis:no-valid-catch-type-found
        } catch (BadRequest400Exception) {
        }
    }

    public function deleteIndex(string $name): void
    {
        try {
            $deleteParams = [
                'index' => $name,
            ];
            // @mago-ignore analysis:invalid-method-access
            // @mago-ignore analysis:mixed-method-access
            $this->client()->indices()->delete($deleteParams);

            // @mago-ignore analysis:non-existent-catch-type
            // @mago-ignore analysis:no-valid-catch-type-found
        } catch (Missing404Exception) {
        }
    }

    public function loadRows(Rows $rows, string $index, IdFactory $idFactory): void
    {
        to_es_bulk_index($this->clientConfig(), $index, $idFactory, [
            'refresh' => true,
        ])->load($rows, flow_context(config()));
    }

    public function version(): int
    {
        return 7;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Context;

use function Flow\ETL\Adapter\Elasticsearch\to_es_bulk_index;
use Elasticsearch\Common\Exceptions\{BadRequest400Exception, Missing404Exception};
use Elasticsearch\{Client, ClientBuilder};
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\{Config, FlowContext, Rows};

final class Elasticsearch7Context implements ElasticsearchContext
{
    private ?Client $client = null;

    public function __construct(private readonly array $hosts)
    {
    }

    public function client() : Client
    {
        if ($this->client === null) {
            $this->client = ClientBuilder::fromConfig($this->clientConfig());
        }

        return $this->client;
    }

    public function clientConfig() : array
    {
        return [
            'hosts' => $this->hosts,
        ];
    }

    public function createIndex(string $name) : void
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

            $response = $this->client()->indices()->create($params);
        } catch (BadRequest400Exception) {
        }
    }

    public function deleteIndex(string $name) : void
    {
        try {
            $deleteParams = [
                'index' => $name,
            ];
            $response = $this->client()->indices()->delete($deleteParams);
        } catch (Missing404Exception) {
        }
    }

    public function loadRows(Rows $rows, string $index, IdFactory $idFactory) : void
    {
        to_es_bulk_index(
            $this->clientConfig(),
            $index,
            $idFactory,
            ['refresh' => true]
        )
            ->load($rows, new FlowContext(Config::default()));
    }

    public function version() : int
    {
        return 7;
    }
}

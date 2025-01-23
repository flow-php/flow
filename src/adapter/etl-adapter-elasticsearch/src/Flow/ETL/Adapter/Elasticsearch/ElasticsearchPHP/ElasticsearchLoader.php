<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\{FlowContext, Loader, Row, Rows};

final class ElasticsearchLoader implements Loader
{
    /** @phpstan-ignore-next-line */
    private \Elasticsearch\Client|\Elastic\Elasticsearch\Client|null $client;

    private string $method;

    /**
     * @var array<mixed>
     */
    private array $parameters = [];

    /**
     * @param array{hosts?: array<string>, connectionParams?: array<mixed>, retries?: int, sniffOnStart?: bool, sslCert?: array<string>, sslKey?: array<string>, sslVerification?: (bool|string), elasticMetaHeader?: bool, includePortInHostHeader?: bool} $config
     */
    public function __construct(
        private readonly array $config,
        private readonly string $index,
        private readonly IdFactory $idFactory,
    ) {
        $this->client = null;
        $this->method = 'index';
    }

    /**
     * @param array{
     *  hosts?: array<string>,
     *  connectionParams?: array<mixed>,
     *  retries?: int,
     *  sniffOnStart?: boolean,
     *  sslCert?: array<string>,
     *  sslKey?: array<string>,
     *  sslVerification?: boolean|string,
     *  elasticMetaHeader?: boolean,
     *  includePortInHostHeader?: boolean
     * } $clientConfig
     */
    public static function update(array $clientConfig, string $index, IdFactory $idFactory) : self
    {
        $loader = new self($clientConfig, $index, $idFactory);
        $loader->method = 'update';

        return $loader;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $factory = $this->idFactory;
        $parameters = $this->parameters;
        $parameters['body'] = [];

        /**
         * @var array<int, array{body:array,id:string}> $dataCollection
         */
        $dataCollection = $rows->map(fn (Row $row) : Row => Row::create(
            $factory->create($row),
            new Row\Entry\JsonEntry('body', $row->toArray())
        ))->toArray();

        foreach ($dataCollection as $data) {
            $parameters['body'][] = [
                $this->method => [
                    '_id' => $data['id'],
                    '_index' => $this->index,
                ],
            ];

            if ($this->method === 'update') {
                $parameters['body'][] = ['doc' => $data['body']];
            } else {
                $parameters['body'][] = $data['body'];
            }
        }

        /**
         * @phpstan-ignore-next-line
         */
        $this->client()->bulk($parameters);
    }

    public function withParameters(array $parameters) : self
    {
        $this->parameters = $parameters;

        return $this;
    }

    /**
     * @phpstan-ignore-next-line
     */
    private function client() : \Elasticsearch\Client|\Elastic\Elasticsearch\Client
    {
        if ($this->client === null) {
            if (\class_exists("Elasticsearch\ClientBuilder")) {
                $this->client = \Elasticsearch\ClientBuilder::fromConfig($this->config);
            } else {
                $this->client = \Elastic\Elasticsearch\ClientBuilder::fromConfig($this->config);
            }
        }

        /**
         * @phpstan-ignore-next-line
         */
        return $this->client;
    }
}

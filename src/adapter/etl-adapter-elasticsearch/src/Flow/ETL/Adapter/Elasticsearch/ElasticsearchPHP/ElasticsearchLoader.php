<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @psalm-suppress UndefinedClass
 *
 * @implements Loader<array{
 *  config: array{
 *    hosts?: array<string>,
 *    connectionParams?: array<mixed>,
 *    retries?: int,
 *    sniffOnStart?: boolean,
 *    sslCert?: array<string>,
 *    sslKey?: array<string>,
 *    sslVerification?: boolean|string,
 *    elasticMetaHeader?: boolean,
 *    includePortInHostHeader?: boolean
 *  },
 *  index: string,
 *  id_factory: IdFactory,
 *  parameters: array<mixed>,
 *  method: string
 * }>
 */
final class ElasticsearchLoader implements Loader, Loader\BatchLoader
{
    /** @phpstan-ignore-next-line */
    private \Elasticsearch\Client|\Elastic\Elasticsearch\Client|null $client;

    private string $method;

    /**
     * @param array{hosts?: array<string>, connectionParams?: array<mixed>, retries?: int, sniffOnStart?: boolean, sslCert?: array<string>, sslKey?: array<string>, sslVerification?: (boolean|string), elasticMetaHeader?: boolean, includePortInHostHeader?: boolean} $config
     * @param array<mixed> $parameters
     */
    public function __construct(
        private array $config,
        private string $index,
        private IdFactory $idFactory,
        private array $parameters = []
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
     * @param array<mixed> $parameters
     */
    public static function update(array $clientConfig, string $index, IdFactory $idFactory, array $parameters = []) : self
    {
        $loader = new self($clientConfig, $index, $idFactory, $parameters);
        $loader->method = 'update';

        return $loader;
    }

    public function __serialize() : array
    {
        return [
            'config' => $this->config,
            'index' => $this->index,
            'id_factory' => $this->idFactory,
            'parameters' => $this->parameters,
            'method' => $this->method,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->config = $data['config'];
        $this->index = $data['index'];
        $this->idFactory = $data['id_factory'];
        $this->parameters = $data['parameters'];
        $this->method = $data['method'];
        $this->client = null;
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
            new Row\Entry\ArrayEntry('body', $row->map(
                function (Row\Entry $entry) : Row\Entry {
                    if ($entry instanceof Row\Entry\JsonEntry) {
                        return new Row\Entry\ArrayEntry($entry->name(), (array) \json_decode($entry->value(), true, 512, JSON_THROW_ON_ERROR));
                    }

                    return $entry;
                }
            )->toArray())
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
         * @psalm-suppress UndefinedClass
         * @psalm-suppress InvalidArgument
         *
         * @phpstan-ignore-next-line
         */
        $this->client()->bulk($parameters);
    }

    /**
     * @psalm-suppress UndefinedClass
     *
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

        return $this->client;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elasticsearch\Client;
use Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
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
 *  chunk_size: int,
 *  index: string,
 *  id_factory: IdFactory,
 *  parameters: array<mixed>,
 *  method: string
 * }>
 */
final class ElasticsearchLoader implements Loader
{
    private ?Client $client;

    private string $method;

    /**
     * @param array{hosts?: array<string>, connectionParams?: array<mixed>, retries?: int, sniffOnStart?: boolean, sslCert?: array<string>, sslKey?: array<string>, sslVerification?: (boolean | string), elasticMetaHeader?: boolean, includePortInHostHeader?: boolean} $config
     * @param array<mixed> $parameters
     */
    public function __construct(private array $config, private int $chunkSize, private string $index, private IdFactory $idFactory, private array $parameters = [])
    {
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
    public static function update(array $clientConfig, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = []) : self
    {
        $loader = new self($clientConfig, $chunkSize, $index, $idFactory, $parameters);
        $loader->method = 'update';

        return $loader;
    }

    public function __serialize() : array
    {
        return [
            'config' => $this->config,
            'chunk_size' => $this->chunkSize,
            'index' => $this->index,
            'id_factory' => $this->idFactory,
            'parameters' => $this->parameters,
            'method' => $this->method,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->config = $data['config'];
        $this->chunkSize = $data['chunk_size'];
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

        foreach ($rows->chunks($this->chunkSize) as $chunk) {
            $parameters = $this->parameters;
            $parameters['body'] = [];

            /**
             * @var array<int, array{body:array,id:string}> $dataCollection
             */
            $dataCollection = $chunk->map(fn (Row $row) : Row => Row::create(
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
                /** @phpstan-ignore-next-line  */
                $parameters['body'][] = [
                    $this->method => [
                        '_id' => $data['id'],
                        '_index' => $this->index,
                    ],
                ];

                if ($this->method === 'update') {
                    /** @phpstan-ignore-next-line  */
                    $parameters['body'][] = ['doc' => $data['body']];
                } else {
                    /** @phpstan-ignore-next-line  */
                    $parameters['body'][] = $data['body'];
                }
            }

            $this->client()->bulk($parameters);
        }
    }

    private function client() : Client
    {
        if ($this->client === null) {
            $this->client = ClientBuilder::fromConfig($this->config);
        }

        return $this->client;
    }
}

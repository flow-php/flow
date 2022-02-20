<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch;

use Elasticsearch\Client;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;

/**
 * @deprecated
 */
final class ElasticsearchPHPLoader implements Loader
{
    private Client $client;

    private int $chunkSize;

    private string $index;

    private IdFactory $idFactory;

    private array $parameters;

    private string $method;

    /**
     * @param Client $client
     * @param int $chunkSize
     * @param string $index
     * @param IdFactory $idFactory
     * @param array $parameters
     */
    public function __construct(Client $client, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = [])
    {
        $this->client = $client;
        $this->chunkSize = $chunkSize;
        $this->index = $index;
        $this->idFactory = $idFactory;
        $this->parameters = $parameters;
        $this->method = 'index';
    }

    public static function update(Client $client, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = []) : self
    {
        $loader = new self($client, $chunkSize, $index, $idFactory, $parameters);
        $loader->method = 'update';

        return $loader;
    }

    /**
     * @throws RuntimeException
     *
     * @return array<string, mixed>
     */
    public function __serialize() : array
    {
        throw new RuntimeException('ElasticsearchPHPLoader is not serializable, please use ElasticsearchPHP\\ClientLoader');
    }

    public function __unserialize(array $data) : void
    {
        throw new RuntimeException('ElasticsearchPHPLoader is not serializable, please use ElasticsearchPHP\\ClientLoader');
    }

    public function load(Rows $rows) : void
    {
        if (!$rows->count()) {
            return;
        }

        $factory = $this->idFactory;

        foreach ($rows->chunks($this->chunkSize) as $chunk) {
            $parameters = $this->parameters;
            $parameters['body'] = [];

            $dataCollection = $chunk->map(fn (Row $row) : Row => Row::create(
                $factory->create($row),
                new Row\Entry\ArrayEntry('body', $row->map(
                    function (Row\Entry $entry) : Row\Entry {
                        if ($entry instanceof Row\Entry\JsonEntry) {
                            return new Row\Entry\ArrayEntry($entry->name(), (array) \json_decode($entry->value(), true));
                        }

                        return $entry;
                    }
                )->toArray())
            ))->toArray();

            /**
             * @var array<array{body:array,id:string}> $data
             */
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

            $this->client->bulk($parameters);
        }
    }
}

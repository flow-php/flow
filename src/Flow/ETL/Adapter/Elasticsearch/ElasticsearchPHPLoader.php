<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch;

use Elasticsearch\Client;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;

final class ElasticsearchPHPLoader implements Loader
{
    private Client $client;

    private int $chunkSize;

    private string $index;

    private IdFactory $idFactory;

    /**
     * @var array<mixed>
     */
    private array $parameters;

    /**
     * @param Client $client
     * @param int $chunkSize
     * @param string $index
     * @param IdFactory $idFactory
     * @param array<mixed> $parameters
     */
    public function __construct(Client $client, int $chunkSize, string $index, IdFactory $idFactory, array $parameters = [])
    {
        $this->client = $client;
        $this->chunkSize = $chunkSize;
        $this->index = $index;
        $this->idFactory = $idFactory;
        $this->parameters = $parameters;
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
             * @var array<array{data:array<mixed>,id:string}> $data
             */
            foreach ($dataCollection as $data) {
                $parameters['body'][] = [
                    'index' => [
                        '_id' => $data['id'],
                        '_index' => $this->index,
                    ],
                ];
                $parameters['body'][] = $data['body'];
            }

            $this->client->bulk($parameters);
        }
    }
}

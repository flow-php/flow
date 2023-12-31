<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\FlowContext;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Meilisearch\Client;
use Psr\Http\Client\ClientInterface;

final class MeilisearchLoader implements Loader
{
    private Client|null $client = null;

    private string $method;

    /**
     * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
     */
    public function __construct(
        private array $config,
        private string $index,
    ) {
        $this->method = 'index';
    }

    /**
     * @param array{url: string, apiKey: string, httpClient: ?ClientInterface} $config
     */
    public static function update(array $config, string $index) : self
    {
        $loader = new self($config, $index);
        $loader->method = 'update';

        return $loader;
    }

    public function __serialize() : array
    {
        return [
            'config' => $this->config,
            'index' => $this->index,
            'method' => $this->method,
        ];
    }

    public function __unserialize(array $data) : void
    {
        $this->config = $data['config'];
        $this->index = $data['index'];
        $this->method = $data['method'];
        $this->client = null;
    }

    public function load(Rows $rows, FlowContext $context) : void
    {
        if (!$rows->count()) {
            return;
        }

        $dataCollection = $rows->map(fn (Row $row) : Row => Row::create(
            ...$row->map(
                function (Row\Entry $entry) : Row\Entry {
                    if ($entry instanceof Row\Entry\JsonEntry) {
                        return new Row\Entry\ArrayEntry($entry->name(), (array) \json_decode($entry->value(), true, 512, JSON_THROW_ON_ERROR));
                    }

                    return $entry;
                }
            )->entries()
        ))->toArray();

        $promise = $this->client()->index($this->index)->updateDocuments($dataCollection);
        $this->client()->waitForTask($promise['taskUid']);
    }

    private function client() : Client
    {
        if ($this->client === null) {
            $this->client = new Client($this->config['url'], $this->config['apiKey'], $this->config['httpClient'] ?? null);
        }

        return $this->client;
    }
}

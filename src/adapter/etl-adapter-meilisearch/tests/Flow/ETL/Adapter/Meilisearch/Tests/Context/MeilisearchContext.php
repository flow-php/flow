<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\Tests\Context;

use function Flow\ETL\Adapter\Meilisearch\to_meilisearch_bulk_index;
use Flow\ETL\Config;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Meilisearch\Client;

final class MeilisearchContext
{
    private Client|null $client = null;

    public function __construct(private readonly string $url, private readonly string $apiKey)
    {
    }

    public function client() : Client
    {
        if ($this->client === null) {
            $this->client = new Client($this->clientConfig()['url'], $this->clientConfig()['apiKey']);
        }

        return $this->client;
    }

    /**
     * @return array{url: string, apiKey: string}
     */
    public function clientConfig() : array
    {
        return [
            'url' => $this->url,
            'apiKey' => $this->apiKey,
        ];
    }

    public function createIndex(string $name) : void
    {
        $this->deleteIndex($name);

        try {
            $promise = $this->client()->createIndex($name);

            $this->client()->waitForTask($promise['taskUid']);
        } catch (\Exception) {
        }
    }

    public function deleteIndex(string $name) : void
    {
        try {
            $promise = $this->client()->deleteIndex($name);

            $this->client()->waitForTask($promise['taskUid']);
        } catch (\Exception) {
        }
    }

    public function loadRows(Rows $rows, string $index) : void
    {
        to_meilisearch_bulk_index(
            $this->clientConfig(),
            $index
        )
            ->load($rows, new FlowContext(Config::default()));
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Backend;

use CmsIg\Seal\Adapter\Meilisearch\MeilisearchAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema;
use Meilisearch\Client;

use function array_column;
use function is_string;
use function max;

trait MeilisearchBackend
{
    private ?Client $client = null;

    abstract protected function schema(): Schema;

    protected function createEngine(): EngineInterface
    {
        $url = getenv('MEILISEARCH_URL');
        $apiKey = getenv('MEILISEARCH_API_KEY');

        $client = new Client(is_string($url) ? $url : '', is_string($apiKey) ? $apiKey : null);
        $this->client = $client;

        return new Engine(new MeilisearchAdapter($client), $this->schema());
    }

    protected function refresh(): void
    {
        $client = $this->client;

        if ($client === null) {
            return;
        }

        $tasks = $client->getTasks()->getResults();

        if ($tasks === []) {
            return;
        }

        /** @var list<int> $uids */
        $uids = array_column($tasks, 'uid');

        $client->waitForTask(max($uids));
    }
}

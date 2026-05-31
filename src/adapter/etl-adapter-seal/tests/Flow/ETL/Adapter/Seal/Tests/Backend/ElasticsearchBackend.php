<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests\Backend;

use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use CmsIg\Seal\Engine;
use CmsIg\Seal\EngineInterface;
use CmsIg\Seal\Schema\Schema;
use Elastic\Elasticsearch\Client;
use Elastic\Elasticsearch\ClientBuilder;

use function is_string;

trait ElasticsearchBackend
{
    private ?Client $client = null;

    abstract protected function schema(): Schema;

    protected function createEngine(): EngineInterface
    {
        $url = getenv('ELASTICSEARCH_URL');
        $hosts = is_string($url) && $url !== '' ? [$url] : [];

        $client = ClientBuilder::create()->setHosts($hosts)->build();
        $this->client = $client;

        return new Engine(new ElasticsearchAdapter($client), $this->schema());
    }

    protected function refresh(): void
    {
        $this->client?->indices()->refresh();
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Seal\Tests;

use CmsIg\Seal\Adapter\Elasticsearch\ElasticsearchAdapter;
use Elastic\Elasticsearch\ClientBuilder;
use Flow\ETL\Adapter\Seal\Tests\Context\SealContext;
use Flow\ETL\Tests\FlowTestCase;

use function getenv;

abstract class IntegrationTestCase extends FlowTestCase
{
    private ?SealContext $sealContext = null;

    protected function setUp(): void
    {
        if (!getenv('ELASTICSEARCH_URL')) {
            static::markTestSkipped('ELASTICSEARCH_URL environment variable is not set');
        }
    }

    protected function tearDown(): void
    {
        $this->sealContext?->dropIndexes();
        $this->sealContext = null;
    }

    protected function sealContext(): SealContext
    {
        if ($this->sealContext === null) {
            $client = ClientBuilder::create()
                ->setHosts([(string) getenv('ELASTICSEARCH_URL')])
                ->build();

            $refresh = static function () use ($client): void {
                $client->indices()->refresh();
            };

            $this->sealContext = new SealContext(new ElasticsearchAdapter($client), $refresh);
        }

        return $this->sealContext;
    }
}

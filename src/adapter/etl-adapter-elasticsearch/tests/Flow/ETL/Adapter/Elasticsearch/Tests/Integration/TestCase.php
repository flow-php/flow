<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Integration;

use Flow\ETL\Adapter\Elasticsearch\Tests\Context\ElasticsearchContext;

abstract class TestCase extends \PHPUnit\Framework\TestCase
{
    protected ElasticsearchContext $elasticsearchContext;

    protected function setUp() : void
    {
        $this->elasticsearchContext = new ElasticsearchContext([\getenv('ELASTICSEARCH_URL')]);
    }
}

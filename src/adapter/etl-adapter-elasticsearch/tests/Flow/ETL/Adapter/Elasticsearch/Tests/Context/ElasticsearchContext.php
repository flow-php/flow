<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Context;

interface ElasticsearchContext
{
    public function clientConfig() : array;

    public function createIndex(string $name) : void;

    public function deleteIndex(string $name) : void;
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\Tests\Context;

use Flow\ETL\Adapter\Elasticsearch\IdFactory;
use Flow\ETL\Rows;

interface ElasticsearchContext
{
    /**
     * @return array{hosts?: array<string>, connectionParams?: array<mixed>, retries?: int, sniffOnStart?: bool, sslCert?: array<string>, sslKey?: array<string>, sslVerification?: bool|string, elasticMetaHeader?: bool, includePortInHostHeader?: bool}
     */
    public function clientConfig(): array;

    public function createIndex(string $name): void;

    public function deleteIndex(string $name): void;

    public function loadRows(Rows $rows, string $index, IdFactory $idFactory): void;

    public function version(): int;
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

enum DocumentDataSource
{
    case fields;
    case source;
}

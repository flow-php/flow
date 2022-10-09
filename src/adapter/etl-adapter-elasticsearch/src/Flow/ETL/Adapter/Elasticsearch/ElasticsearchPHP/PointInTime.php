<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elastic\Elasticsearch\Response\Elasticsearch;

final class PointInTime
{
    /**
     * @var array{id: string}
     */
    private array $pit;

    /**
     * @param array{id: string}|Elasticsearch $pit
     */
    public function __construct(array|Elasticsearch $pit)
    {
        $this->pit = \is_array($pit) ? $pit : $pit->asArray();
    }

    public function id() : string
    {
        return $this->pit['id'];
    }
}

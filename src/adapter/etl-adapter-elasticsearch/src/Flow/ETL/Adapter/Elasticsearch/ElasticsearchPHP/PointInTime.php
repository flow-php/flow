<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elastic\Elasticsearch\Response\Elasticsearch;

use function is_array;

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
        if (is_array($pit)) {
            $this->pit = $pit;
        } else {
            /** @var array{id: string} $data */
            $data = $pit->asArray();
            $this->pit = $data;
        }
    }

    public function id(): string
    {
        return $this->pit['id'];
    }
}

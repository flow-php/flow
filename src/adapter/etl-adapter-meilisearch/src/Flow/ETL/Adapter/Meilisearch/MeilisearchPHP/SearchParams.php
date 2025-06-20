<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

final class SearchParams
{
    /**
     * @var array{q: string, limit: int, offset: int, attributesToRetrieve?: null|array<string>, sort?: null|array<string>}
     */
    private array $params;

    private readonly string $query;

    /**
     * @param array{q: string, limit?: null|int, offset?: null|int, attributesToRetrieve?: null|array<string>, sort?: null|array<string>} $params See: https://www.meilisearch.com/docs/reference/api/search#search-parameters
     */
    public function __construct(array $params)
    {
        $this->query = $params['q'];

        // Build params array with guaranteed non-null limit and offset
        $mergedParams = \array_merge(['limit' => 20, 'offset' => 0, 'q' => $params['q']], $params);

        $this->params = [
            'q' => $mergedParams['q'],
            'limit' => (int) ($mergedParams['limit'] ?? 20),
            'offset' => (int) ($mergedParams['offset'] ?? 0),
        ];

        // Add optional parameters if they exist
        if (isset($mergedParams['attributesToRetrieve'])) {
            $this->params['attributesToRetrieve'] = $mergedParams['attributesToRetrieve'];
        }

        if (isset($mergedParams['sort'])) {
            $this->params['sort'] = $mergedParams['sort'];
        }
    }

    /**
     * @return array{q: string, limit: int, offset: int, attributesToRetrieve?: null|array<string>, sort?: null|array<string>}
     */
    public function asArray() : array
    {
        return $this->params;
    }

    public function getLimit() : int
    {
        return $this->params['limit'];
    }

    public function getOffset() : int
    {
        return $this->params['offset'];
    }

    public function getQuery() : string
    {
        return $this->query;
    }

    public function set(string $key, mixed $value) : self
    {
        /**
         * @phpstan-ignore-next-line
         */
        return new self(\array_merge($this->params, [$key => $value]));
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

final class SearchParams
{
    private readonly string $query;

    /**
     * @param array{q: string, limit: ?int, offset: ?int, attributesToRetrieve: ?array<string>, sort: ?array<string>} $params See: https://www.meilisearch.com/docs/reference/api/search#search-parameters
     */
    public function __construct(private array $params)
    {
        $this->query = $params['q'];
        $this->params = \array_merge(['limit' => 20, 'offset' => 0], $params);
    }

    /**
     * @return array{q: string, limit: ?int, offset: ?int, attributesToRetrieve: ?array<string>, sort: ?array<string>}
     */
    public function asArray() : array
    {
        return $this->params;
    }

    public function getQuery() : string
    {
        return $this->query;
    }

    public function set(string $key, mixed $value) : self
    {
        /**
         * @psalm-suppress InvalidArgument
         *
         * @phpstan-ignore-next-line
         */
        return new self(\array_merge($this->params, [$key => $value]));
    }
}

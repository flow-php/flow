<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

final class SearchParams
{
    /**
     * @param array<mixed> $params - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
     */
    public function __construct(private readonly array $params)
    {
    }

    /**
     * @return array<mixed>
     */
    public function asArray() : array
    {
        return $this->params;
    }

    public function hasSort() : bool
    {
        if (\array_key_exists('body', $this->params)) {
            if (\array_key_exists('sort', $this->params['body'])) {
                return true;
            }
        }

        return \array_key_exists('sort', $this->params);
    }

    public function set(string $key, mixed $value) : self
    {
        return new self(\array_merge($this->params, [$key => $value]));
    }

    public function setBody(string $key, mixed $value) : self
    {
        return $this->set('body', \array_merge($this->params['body'], [$key => $value]));
    }
}

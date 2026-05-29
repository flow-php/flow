<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use function array_key_exists;
use function array_merge;
use function is_array;

final readonly class SearchParams
{
    /**
     * @param array<mixed> $params - https://www.elastic.co/guide/en/elasticsearch/reference/master/search-search.html
     */
    public function __construct(
        private array $params,
    ) {}

    /**
     * @return array<mixed>
     */
    public function asArray(): array
    {
        return $this->params;
    }

    public function hasSort(): bool
    {
        if (array_key_exists('body', $this->params) && is_array($this->params['body'])) {
            if (array_key_exists('sort', $this->params['body'])) {
                return true;
            }
        }

        return array_key_exists('sort', $this->params);
    }

    public function remove(string $key): self
    {
        $params = $this->params;

        if (array_key_exists($key, $params)) {
            unset($params[$key]);
        }

        return new self($params);
    }

    public function set(string $key, mixed $value): self
    {
        return new self(array_merge($this->params, [$key => $value]));
    }

    public function setBody(string $key, mixed $value): self
    {
        /** @var array<string, mixed> $body */
        $body = array_key_exists('body', $this->params) && is_array($this->params['body']) ? $this->params['body'] : [];

        return $this->set('body', array_merge($body, [$key => $value]));
    }
}

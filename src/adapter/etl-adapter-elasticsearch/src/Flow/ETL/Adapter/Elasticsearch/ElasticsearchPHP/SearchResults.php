<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elastic\Elasticsearch\Response\Elasticsearch;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Rows;

final class SearchResults
{
    /**
     * @var array<mixed>
     */
    private readonly array $results;

    /**
     * @param array<mixed>|Elasticsearch $results
     */
    public function __construct(array|Elasticsearch $results)
    {
        $this->results = (\is_array($results)) ? $results : $results->asArray();
    }

    public function lastHitSort() : ?array
    {
        if (!$this->size()) {
            return null;
        }

        $hits = $this->results['hits']['hits'];

        $lastHit = \end($hits);

        return \array_key_exists('sort', $lastHit) ? $lastHit['sort'] : null;
    }

    public function pages() : int
    {
        if ($this->size() === 0) {
            return 0;
        }

        return (int) \ceil($this->total() / $this->size());
    }

    public function size() : int
    {
        return \count($this->results['hits']['hits']);
    }

    public function toRows(EntryFactory $entryFactory, ?Row\Schema $schema = null) : Rows
    {
        /** @var array<Row\Entry> $entries */
        $entries = [];

        foreach (\array_keys($this->results) as $key) {
            $entries[$key] = $entryFactory->create($key, $this->results[$key], $schema);
        }

        return new Rows(Row::create(...$entries));
    }

    public function total() : int
    {
        return (int) $this->results['hits']['total']['value'];
    }
}

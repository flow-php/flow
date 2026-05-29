<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Elasticsearch\ElasticsearchPHP;

use Elastic\Elasticsearch\Response\Elasticsearch;
use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Rows;

use function array_key_exists;
use function ceil;
use function count;
use function end;
use function is_array;

final readonly class SearchResults
{
    /**
     * @var array{hits: array{hits: array<int, array<string, mixed>>, total: array{value: int, ...}, ...}, ...}
     */
    private array $results;

    /**
     * @param array{hits: array{hits: array<int, array<string, mixed>>, total: array{value: int, ...}, ...}, ...}|Elasticsearch $results
     */
    public function __construct(array|Elasticsearch $results)
    {
        /** @var array{hits: array{hits: array<int, array<string, mixed>>, total: array{value: int, ...}, ...}, ...} $data */
        $data = is_array($results) ? $results : $results->asArray();
        $this->results = $data;
    }

    /**
     * @return array<mixed>|null
     */
    public function lastHitSort(): ?array
    {
        if (!$this->size()) {
            return null;
        }

        $hits = $this->results['hits']['hits'];

        $lastHit = end($hits);

        if (!is_array($lastHit) || !array_key_exists('sort', $lastHit)) {
            return null;
        }

        /** @var array<mixed> */
        return $lastHit['sort'];
    }

    public function pages(): int
    {
        if ($this->size() === 0) {
            return 0;
        }

        return (int) ceil($this->total() / $this->size());
    }

    public function size(): int
    {
        return count($this->results['hits']['hits']);
    }

    public function toRows(EntryFactory $entryFactory): Rows
    {
        /** @var array<string, Row\Entry> $entries */
        $entries = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->results as $key => $value) {
            $entries[(string) $key] = $entryFactory->create((string) $key, $value);
        }

        return new Rows(Row::create(...$entries));
    }

    public function total(): int
    {
        return $this->results['hits']['total']['value'];
    }
}

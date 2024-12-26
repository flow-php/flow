<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\Row\EntryFactory;
use Flow\ETL\{Row, Rows};
use Meilisearch\Search\SearchResult;

final class SearchResults
{
    public function __construct(private readonly SearchResult $results)
    {
    }

    public function pages(int $limit) : int
    {
        return (int) \ceil($this->total() / $limit);
    }

    public function size() : int
    {
        return $this->results->getHitsCount();
    }

    public function toRows(EntryFactory $entryFactory) : Rows
    {
        $newRows = [];

        foreach ($this->results->getHits() as $hit) {
            /** @var array<Row\Entry> $entries */
            $entries = [];

            foreach ($hit as $key => $value) {
                $entries[$key] = $entryFactory->create((string) $key, $value);
            }

            $newRows[] = Row::create(...$entries);
        }

        return new Rows(...$newRows);
    }

    public function total() : int
    {
        // Estimated total hits are set only in paginated result list
        if (null !== $this->results->getOffset()) {
            return $this->results->getEstimatedTotalHits() ?: $this->results->getHitsCount() ?: 0;
        }

        return $this->results->getTotalHits() ?: $this->results->getHitsCount() ?: 0;
    }
}

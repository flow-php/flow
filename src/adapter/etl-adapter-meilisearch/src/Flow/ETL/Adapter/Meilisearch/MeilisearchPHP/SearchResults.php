<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\Meilisearch\MeilisearchPHP;

use Flow\ETL\Row;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Rows;
use Meilisearch\Search\SearchResult;

final class SearchResults
{
    public function __construct(private readonly SearchResult $results)
    {
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
        try {
            /** @phpstan-ignore-next-line */
            return $this->results->getTotalHits();
        } catch (\Error) {
            return $this->results->getEstimatedTotalHits();
        }
    }
}

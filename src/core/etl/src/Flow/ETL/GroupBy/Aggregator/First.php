<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class First  implements Aggregator
{
    private readonly EntryReference $entry;

    private ?Entry $first;

    public function __construct(string|EntryReference $entry)
    {
        $this->entry = \is_string($entry) ? new EntryReference($entry) : $entry;
        $this->first = null;
    }

    public function aggregate(Row $row) : void
    {
        if ($this->first === null) {
            try {
                $this->first = $row->get($this->entry->to());
            } catch (InvalidArgumentException $e) {
                // entry not found
            }
        }
    }

    public function result() : Entry
    {
        return $this->first ?? new Entry\NullEntry($this->entry->name());
    }
}

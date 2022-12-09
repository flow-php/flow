<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class Count implements Aggregator
{
    private int $count;

    private readonly EntryReference $entry;

    public function __construct(string|EntryReference $entry)
    {
        $this->entry = \is_string($entry) ? new EntryReference($entry) : $entry;
        $this->count = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $row->valueOf($this->entry->to());
            $this->count += 1;
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->entry->hasAlias()) {
            $this->entry->as($this->entry->to() . '_count');
        }

        return \Flow\ETL\DSL\Entry::integer($this->entry->name(), $this->count);
    }
}

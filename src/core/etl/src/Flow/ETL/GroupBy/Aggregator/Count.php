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

    private readonly EntryReference $ref;

    public function __construct(string|EntryReference $entry)
    {
        $this->ref = EntryReference::init($entry);
        $this->count = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $row->valueOf($this->ref);
            $this->count++;
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_count');
        }

        return \Flow\ETL\DSL\Entry::integer($this->ref->name(), $this->count);
    }
}

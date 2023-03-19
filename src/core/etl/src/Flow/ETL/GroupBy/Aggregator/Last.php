<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class Last  implements Aggregator
{
    private ?Entry $last;

    private readonly EntryReference $ref;

    public function __construct(string|EntryReference $entry)
    {
        $this->ref = EntryReference::init($entry);
        $this->last = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $this->last = $row->get($this->ref);
        } catch (InvalidArgumentException $e) {
            // entry not found
        }
    }

    public function result() : Entry
    {
        return $this->last ?? new Entry\NullEntry($this->ref->name());
    }
}

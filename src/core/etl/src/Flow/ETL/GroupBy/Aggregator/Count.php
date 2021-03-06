<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Count implements Aggregator
{
    private int $count;

    public function __construct(private readonly string $entry)
    {
        $this->count = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $row->valueOf($this->entry);
            $this->count += 1;
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        return \Flow\ETL\DSL\Entry::integer($this->entry . '_count', $this->count);
    }
}

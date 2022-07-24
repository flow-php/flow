<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Last  implements Aggregator
{
    private ?Entry $last;

    public function __construct(private readonly string $entry)
    {
        $this->last = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $this->last = $row->get($this->entry);
        } catch (InvalidArgumentException $e) {
            // entry not found
        }
    }

    public function result() : Entry
    {
        return $this->last ?? new Entry\NullEntry($this->entry);
    }
}

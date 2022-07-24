<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class First  implements Aggregator
{
    private ?Entry $first;

    public function __construct(private readonly string $entry)
    {
        $this->first = null;
    }

    public function aggregate(Row $row) : void
    {
        if ($this->first === null) {
            try {
                $this->first = $row->get($this->entry);
            } catch (InvalidArgumentException $e) {
                // entry not found
            }
        }
    }

    public function result() : Entry
    {
        return $this->first ?? new Entry\NullEntry($this->entry);
    }
}

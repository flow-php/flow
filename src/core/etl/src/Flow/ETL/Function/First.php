<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;

final class First implements AggregatingFunction
{
    private ?Entry $first;

    public function __construct(private readonly Reference $ref)
    {
        $this->first = null;
    }

    public function aggregate(Row $row) : void
    {
        if ($this->first === null) {
            try {
                $this->first = $row->get($this->ref);
            } catch (InvalidArgumentException $e) {
                // entry not found
            }
        }
    }

    public function result() : Entry
    {
        return $this->first ?? new Entry\NullEntry($this->ref->name());
    }
}

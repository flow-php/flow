<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;

final class Last implements AggregatingFunction
{
    private ?Entry $last;

    public function __construct(private readonly Reference $ref)
    {
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

<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class Average implements Aggregator
{
    private int $count;

    private readonly EntryReference $ref;

    private float $sum;

    public function __construct(string|EntryReference $entry)
    {
        $this->ref = EntryReference::init($entry);
        $this->count = 0;
        $this->sum = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if (\is_numeric($value)) {
                $this->sum += $value;
                $this->count++;
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_avg');
        }

        if (0 !== $this->count) {
            $result = $this->sum / $this->count;
            $resultInt = (int) $result;
        } else {
            $result = 0.0;
            $resultInt = 0;
        }

        if ($result - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->ref->name(), (int) $result);
        }

        return \Flow\ETL\DSL\Entry::float($this->ref->name(), $result);
    }
}

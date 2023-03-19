<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class Min implements Aggregator
{
    private ?float $min;

    private readonly EntryReference $ref;

    public function __construct(string|EntryReference $entry)
    {
        $this->ref = EntryReference::init($entry);
        $this->min = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if ($this->min === null) {
                if (\is_numeric($value)) {
                    $this->min = (float) $value;
                }
            } else {
                if (\is_numeric($value)) {
                    $this->min = \min($this->min, (float) $value);
                }
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_min');
        }

        $resultInt = (int) $this->min;

        if ($this->min === null) {
            return \Flow\ETL\DSL\Entry::null($this->ref->name());
        }

        if ($this->min - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->ref->name(), (int) $this->min);
        }

        return \Flow\ETL\DSL\Entry::float($this->ref->name(), $this->min);
    }
}

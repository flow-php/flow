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
    private readonly EntryReference $entry;

    private ?float $min;

    public function __construct(string|EntryReference $entry)
    {
        $this->entry = \is_string($entry) ? new EntryReference($entry) : $entry;
        $this->min = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->entry->to());

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
        if (!$this->entry->hasAlias()) {
            $this->entry->as($this->entry->to() . '_min');
        }

        $resultInt = (int) $this->min;

        if ($this->min === null) {
            return \Flow\ETL\DSL\Entry::null($this->entry->name());
        }

        if ($this->min - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->entry->name(), (int) $this->min);
        }

        return \Flow\ETL\DSL\Entry::float($this->entry->name(), $this->min);
    }
}

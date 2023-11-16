<?php

declare(strict_types=1);

namespace Flow\ETL\Row\Reference\Expression;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryReference;

final class Max implements Aggregator
{
    private ?float $max;

    private readonly EntryReference $ref;

    public function __construct(string|EntryReference $entry)
    {
        $this->ref = EntryReference::init($entry);
        $this->max = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if ($this->max === null) {
                if (\is_numeric($value)) {
                    $this->max = (float) $value;
                }
            } else {
                if (\is_numeric($value)) {
                    $this->max = \max($this->max, (float) $value);
                }
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_max');
        }

        if ($this->max === null) {
            return \Flow\ETL\DSL\Entry::null($this->ref->name());
        }

        $resultInt = (int) $this->max;

        if ($this->max - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->ref->name(), (int) $this->max);
        }

        return \Flow\ETL\DSL\Entry::float($this->ref->name(), $this->max);
    }
}

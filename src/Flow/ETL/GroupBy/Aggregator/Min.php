<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Min implements Aggregator
{
    private string $entry;

    private ?float $min;

    public function __construct(string $entry)
    {
        $this->entry = $entry;
        $this->min = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->entry);

            if ($this->min === null) {
                if (\is_numeric($value)) {
                    $this->min = (float) $value;
                }
            } else {
                if (\is_numeric($value)) {
                    $this->min = \min($this->min, (float) $value);
                }
            }
        } catch (InvalidArgumentException $e) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        $resultInt = (int) $this->min;

        if ($this->min === null) {
            return \Flow\ETL\DSL\Entry::null($this->entry . '_min');
        }

        if ($this->min - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->entry . '_min', (int) $this->min);
        }

        return \Flow\ETL\DSL\Entry::float($this->entry . '_min', $this->min);
    }
}

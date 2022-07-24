<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Max implements Aggregator
{
    private ?float $max;

    public function __construct(private readonly string $entry)
    {
        $this->max = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->entry);

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
        if ($this->max === null) {
            return \Flow\ETL\DSL\Entry::null($this->entry . '_max');
        }

        $resultInt = (int) $this->max;

        if ($this->max - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->entry . '_max', (int) $this->max);
        }

        return \Flow\ETL\DSL\Entry::float($this->entry . '_max', $this->max);
    }
}

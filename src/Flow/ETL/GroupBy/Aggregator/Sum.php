<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Sum implements Aggregator
{
    private string $entry;

    private float $sum;

    public function __construct(string $entry)
    {
        $this->entry = $entry;
        $this->sum = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->entry);

            if (\is_numeric($value)) {
                $this->sum += $value;
            }
        } catch (InvalidArgumentException $e) {
            // do nothing?
        }
    }

    public function result() : Entry
    {
        $resultInt = (int) $this->sum;

        if ($this->sum - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->entry . '_sum', (int) $this->sum);
        }

        return \Flow\ETL\DSL\Entry::float($this->entry . '_sum', $this->sum);
    }
}

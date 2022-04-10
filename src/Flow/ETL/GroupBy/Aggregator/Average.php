<?php

declare(strict_types=1);

namespace Flow\ETL\GroupBy\Aggregator;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\GroupBy\Aggregator;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;

final class Average implements Aggregator
{
    private int $count;

    private float $sum;

    public function __construct(private readonly string $entry)
    {
        $this->count = 0;
        $this->sum = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->entry);

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
        $result = $this->sum / $this->count;
        $resultInt = (int) $result;

        if ($result - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->entry . '_avg', (int) $result);
        }

        return \Flow\ETL\DSL\Entry::float($this->entry . '_avg', $result);
    }
}

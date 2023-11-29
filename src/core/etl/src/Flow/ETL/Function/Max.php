<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function Flow\ETL\DSL\null_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;

final class Max implements AggregatingFunction
{
    private ?float $max;

    public function __construct(private readonly Reference $ref)
    {
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
            return null_entry($this->ref->name());
        }

        $resultInt = (int) $this->max;

        if ($this->max - $resultInt === 0.0) {
            return int_entry($this->ref->name(), (int) $this->max);
        }

        return float_entry($this->ref->name(), $this->max);
    }
}

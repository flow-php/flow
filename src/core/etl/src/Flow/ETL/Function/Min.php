<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{datetime_entry, float_entry, int_entry};
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Row;
use Flow\ETL\Row\{Entry, Reference};

final class Min implements AggregatingFunction
{
    private float|\DateTimeInterface|null $min;

    public function __construct(private readonly Reference $ref)
    {
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
                } elseif ($value instanceof \DateTimeInterface) {
                    $this->min = $value;
                }
            } else {
                if (\is_numeric($value)) {
                    $this->min = \min($this->min, (float) $value);
                } elseif ($value instanceof \DateTimeInterface) {
                    $this->min = \min($this->min, $value);
                }
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    /**
     * @return Entry<?\DateTimeInterface, \DateTimeInterface>|Entry<?float, float>|Entry<?int, int>
     */
    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_min');
        }

        if ($this->min === null) {
            return int_entry($this->ref->name(), null);
        }

        if ($this->min instanceof \DateTimeInterface) {
            return datetime_entry($this->ref->name(), $this->min);
        }

        $resultInt = (int) $this->min;

        if ($this->min - $resultInt === 0.0) {
            return int_entry($this->ref->name(), (int) $this->min);
        }

        return float_entry($this->ref->name(), $this->min);
    }
}

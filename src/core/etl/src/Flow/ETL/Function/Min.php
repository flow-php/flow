<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\Types\Type\Native\FloatType;

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function is_numeric;
use function min;

final class Min implements AggregatingFunction
{
    private bool $floatColumn = false;

    private float|DateTimeInterface|null $min;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->min = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        $entry = $row->get($this->ref);

        if (!$this->floatColumn && $entry->definition()->type() instanceof FloatType) {
            $this->floatColumn = true;
        }

        try {
            /** @var mixed $value */
            $value = $entry->value();

            if ($this->min === null) {
                if (is_numeric($value)) {
                    $this->min = (float) $value;
                } elseif ($value instanceof DateTimeInterface) {
                    $this->min = $value;
                }
            } else {
                if (is_numeric($value)) {
                    $this->min = min($this->min, (float) $value);
                } elseif ($value instanceof DateTimeInterface) {
                    $this->min = min($this->min, $value);
                }
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Min error: ' . $e->getMessage(), 0, $e);
        }
    }

    /**
     * @return Entry<?\DateTimeInterface>|Entry<?float>|Entry<?int>
     */
    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function result(EntryFactory $entryFactory): Entry
    {
        $ref = $this->ref->hasAlias() ? $this->ref : $this->ref->as($this->ref->to() . '_min');

        if ($this->floatColumn) {
            return float_entry($ref->name(), $this->min instanceof DateTimeInterface ? null : $this->min);
        }

        if ($this->min === null) {
            return int_entry($ref->name(), null);
        }

        if ($this->min instanceof DateTimeInterface) {
            return datetime_entry($ref->name(), $this->min);
        }

        $resultInt = (int) $this->min;

        if (($this->min - $resultInt) === 0.0) {
            return int_entry($ref->name(), (int) $this->min);
        }

        return float_entry($ref->name(), $this->min);
    }
}

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

use function Flow\ETL\DSL\datetime_entry;
use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function is_numeric;
use function min;

final class Min implements AggregatingFunction
{
    private float|DateTimeInterface|null $min;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->min = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

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
            $context->functions()->invalidResult(new InvalidArgumentException('Min error: ' . $e->getMessage()));
        }
    }

    /**
     * @return Entry<?\DateTimeInterface>|Entry<?float>|Entry<?int>
     */
    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_min');
        }

        if ($this->min === null) {
            return int_entry($this->ref->name(), null);
        }

        if ($this->min instanceof DateTimeInterface) {
            return datetime_entry($this->ref->name(), $this->min);
        }

        $resultInt = (int) $this->min;

        if (($this->min - $resultInt) === 0.0) {
            return int_entry($this->ref->name(), (int) $this->min);
        }

        return float_entry($this->ref->name(), $this->min);
    }
}

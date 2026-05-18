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
use function max;

final class Max implements AggregatingFunction
{
    private float|DateTimeInterface|null $max;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->max = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if ($this->max === null) {
                if (is_numeric($value)) {
                    $this->max = (float) $value;
                } elseif ($value instanceof DateTimeInterface) {
                    $this->max = $value;
                }
            } else {
                if (is_numeric($value)) {
                    $this->max = max($this->max, (float) $value);
                } elseif ($value instanceof DateTimeInterface) {
                    $this->max = max($this->max, $value);
                }
            }
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Max error: ' . $e->getMessage()));
        }
    }

    /**
     * @return Entry<?\DateTimeInterface>|Entry<?float>|Entry<?int>
     */
    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_max');
        }

        if ($this->max === null) {
            return int_entry($this->ref->name(), null);
        }

        if ($this->max instanceof DateTimeInterface) {
            return datetime_entry($this->ref->name(), $this->max);
        }

        $resultInt = (int) $this->max;

        if (($this->max - $resultInt) === 0.0) {
            return int_entry($this->ref->name(), (int) $this->max);
        }

        return float_entry($this->ref->name(), $this->max);
    }
}

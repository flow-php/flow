<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;

final class Sum implements AggregatingFunction, WindowFunction
{
    private float|int $sum;

    private ?Window $window;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->sum = 0;
        $this->window = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        try {
            $entry = $row->get($this->ref);
            $value = $entry->value();

            if (\is_numeric($value)) {
                $this->sum = (new Calculator())->add($this->sum, $value);
            }
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Sum error: ' . $e->getMessage()));
        }
    }

    public function apply(Row $row, Rows $partition, FlowContext $context): mixed
    {
        $sum = 0;

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            try {
                $entry = $partitionRow->get($this->ref);
                $value = $entry->value();

                if (\is_numeric($value)) {
                    $sum = (new Calculator())->add($sum, $value);
                }
            } catch (InvalidArgumentException $e) {
                $context
                    ->functions()
                    ->invalidResult(
                        new InvalidArgumentException('Sum window function error: ' . $e->getMessage(), 0, $e),
                    );
            }
        }

        return $sum;
    }

    public function over(Window $window): WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    /**
     * @return Entry<?float>|Entry<?int>
     */
    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_sum');
        }

        if (!is_float($this->sum)) {
            return int_entry($this->ref->name(), (int) $this->sum);
        }

        return float_entry($this->ref->name(), $this->sum);
    }

    public function toString(): string
    {
        return 'sum()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}

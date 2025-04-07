<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\{float_entry, integer_entry};
use Flow\Calculator\{Calculator, Rounding};
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\{Row, Rows, Window};

final class Average implements AggregatingFunction, WindowFunction
{
    private int $count;

    private float $sum;

    private ?Window $window;

    public function __construct(private readonly Reference $ref, private readonly int $scale = 2, private readonly Rounding $rounding = Rounding::HALF_UP)
    {
        $this->window = null;
        $this->count = 0;
        $this->sum = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if (\is_numeric($value)) {
                $this->sum = (new Calculator())->add($this->sum, $value);
                $this->count++;
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function apply(Row $row, Rows $partition) : mixed
    {
        $sum = 0;
        $count = 0;

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            /** @var mixed $value */
            $value = $partitionRow->valueOf($this->ref);

            if (\is_numeric($value)) {
                $sum = (new Calculator())->add($sum, $value);
                $count++;
            }
        }

        return (new Calculator())->divide($sum, $count, $this->scale, $this->rounding);
    }

    public function over(Window $window) : WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_avg');
        }

        if (0 !== $this->count) {
            $result = (new Calculator())->divide($this->sum, $this->count, $this->scale, $this->rounding);
            $resultInt = (int) $result;
        } else {
            $result = 0;
        }

        if (\is_int($result)) {
            return integer_entry($this->ref->name(), $result);
        }

        return float_entry($this->ref->name(), $result);
    }

    public function toString() : string
    {
        return 'average()';
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}

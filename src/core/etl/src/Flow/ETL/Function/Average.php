<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class Average implements AggregatingFunction, WindowFunction
{
    private int $count;

    private float $sum;

    private ?Window $window;

    public function __construct(private readonly Reference $ref)
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
                $this->sum += $value;
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
                $sum += $value;
                $count++;
            }
        }

        return $sum / $count;
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
            $result = $this->sum / $this->count;
            $resultInt = (int) $result;
        } else {
            $result = 0.0;
            $resultInt = 0;
        }

        if ($result - $resultInt === 0.0) {
            return \Flow\ETL\DSL\Entry::integer($this->ref->name(), (int) $result);
        }

        return \Flow\ETL\DSL\Entry::float($this->ref->name(), $result);
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

<?php declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class Sum implements AggregatingFunction, WindowFunction
{
    private float $sum;

    private ?Window $window;

    public function __construct(private readonly Reference $ref)
    {
        $this->sum = 0;
        $this->window = null;
    }

    public function aggregate(Row $row) : void
    {
        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if (\is_numeric($value)) {
                $this->sum += $value;
            }
        } catch (InvalidArgumentException) {
            // do nothing?
        }
    }

    public function apply(Row $row, Rows $partition) : mixed
    {
        $sum = 0;

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            /** @var mixed $value */
            $value = $partitionRow->valueOf($this->ref);

            if (\is_numeric($value)) {
                $sum += $value;
            }
        }

        return $sum;
    }

    public function over(Window $window) : WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    public function result() : Entry
    {
        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_sum');
        }

        $resultInt = (int) $this->sum;

        if ($this->sum - $resultInt === 0.0) {
            return int_entry($this->ref->name(), (int) $this->sum);
        }

        return float_entry($this->ref->name(), $this->sum);
    }

    public function toString() : string
    {
        return 'sum()';
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}

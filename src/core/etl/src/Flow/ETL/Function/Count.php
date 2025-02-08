<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\{Row, Rows, Window};

final class Count implements AggregatingFunction, WindowFunction
{
    private int $count;

    private ?Window $window;

    public function __construct(private readonly ?Reference $ref = null)
    {
        $this->window = null;
        $this->count = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            if ($this->ref) {
                $row->valueOf($this->ref);
            }
            $this->count++;
        } catch (InvalidArgumentException) {
        }
    }

    public function apply(Row $row, Rows $partition) : mixed
    {
        if ($this->ref === null) {
            throw new RuntimeException('Count WindowFunction function requires a reference.');
        }

        $count = 0;
        $value = $row->valueOf($this->ref);

        foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
            $partitionValue = $partitionRow->valueOf($this->ref);

            if ($partitionValue === $value) {
                $count++;
            }
        }

        return $count;
    }

    public function over(Window $window) : WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    /**
     * @return Entry<?int, ?int>
     */
    public function result() : Entry
    {
        if (!$this->ref) {
            return int_entry('_count', $this->count);
        }

        if (!$this->ref->hasAlias()) {
            $this->ref->as($this->ref->to() . '_count');
        }

        return int_entry($this->ref->name(), $this->count);
    }

    public function toString() : string
    {
        return 'count()';
    }

    public function window() : Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}

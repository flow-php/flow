<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;

final class Count implements AggregatingFunction, WindowFunction
{
    private int $count;

    private ?Window $window;

    public function __construct(private readonly Reference $ref)
    {
        $this->window = null;
        $this->count = 0;
    }

    public function aggregate(Row $row) : void
    {
        try {
            $row->valueOf($this->ref);
            $this->count++;
        } catch (InvalidArgumentException) {
        }
    }

    public function apply(Row $row, Rows $partition) : mixed
    {
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

    public function result() : Entry
    {
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

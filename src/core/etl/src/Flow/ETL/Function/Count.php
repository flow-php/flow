<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\int_entry;
use Flow\ETL\Exception\{InvalidArgumentException, RuntimeException};
use Flow\ETL\{FlowContext, Row, Rows, Window};
use Flow\ETL\Row\{Entry, Reference};
use Flow\ETL\Row\EntryFactory;

final class Count implements AggregatingFunction, WindowFunction
{
    private int $count;

    private ?Window $window;

    public function __construct(private readonly ?Reference $ref = null)
    {
        $this->window = null;
        $this->count = 0;
    }

    public function aggregate(Row $row, FlowContext $context) : void
    {
        try {
            if ($this->ref) {
                $row->valueOf($this->ref);
            }
            $this->count++;
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Count error: ' . $e->getMessage()));
        }
    }

    public function apply(Row $row, Rows $partition, FlowContext $context) : mixed
    {
        if ($this->ref === null) {
            throw new RuntimeException('Count WindowFunction function requires a reference.');
        }

        $count = 0;

        try {
            $value = $row->valueOf($this->ref);

            foreach ($partition->sortBy(...$this->window()->order()) as $partitionRow) {
                try {
                    $partitionValue = $partitionRow->valueOf($this->ref);

                    if ($partitionValue === $value) {
                        $count++;
                    }
                } catch (InvalidArgumentException $e) {
                    $context->functions()->invalidResult(new InvalidArgumentException('Count window function error: ' . $e->getMessage(), 0, $e));
                }
            }
        } catch (InvalidArgumentException $e) {
            return $context->functions()->invalidResult(new InvalidArgumentException('Count window function error: ' . $e->getMessage(), 0, $e));
        }

        return $count;
    }

    public function over(Window $window) : WindowFunction
    {
        $this->window = $window;

        return $this;
    }

    /**
     * @return Entry<?int>
     */
    public function result(EntryFactory $entryFactory) : Entry
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

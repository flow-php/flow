<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\CountAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;

use function Flow\ETL\DSL\int_entry;

final class Count implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    private int $count;

    private ?Window $window;

    public function __construct(
        private readonly ?Reference $ref = null,
    ) {
        $this->window = null;
        $this->count = 0;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if ($this->ref !== null && !$row->has($this->ref)) {
            return;
        }

        if ($this->ref) {
            $row->valueOf($this->ref);
        }
        $this->count++;
    }

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        return new CountAccumulator($this->ref, $context);
    }

    public function apply(WindowContext $window): mixed
    {
        if ($this->ref === null) {
            return $window->frame()->count();
        }

        $accumulator = $this->accumulator($window->flowContext());

        foreach ($window->frame() as $frameRow) {
            $accumulator->accumulate($frameRow);
        }

        return $accumulator->value();
    }

    public function over(Window $window): static
    {
        $this->window = $window;

        return $this;
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return $this->ref === null ? [] : [$this->ref];
    }

    /**
     * @return Entry<?int>
     */
    public function result(EntryFactory $entryFactory): Entry
    {
        if (!$this->ref) {
            return int_entry('_count', $this->count);
        }

        $ref = $this->ref->hasAlias() ? $this->ref : $this->ref->as($this->ref->to() . '_count');

        return int_entry($ref->name(), $this->count);
    }

    public function toString(): string
    {
        return 'count()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}

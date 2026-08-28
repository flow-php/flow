<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\SumAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type\Native\FloatType;

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function is_numeric;

final class Sum implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    private bool $floatColumn = false;

    private int $aggregated = 0;

    private ?RunningSum $runningSum = null;

    private float|int $sum;

    private ?Window $window;

    public function __construct(
        private readonly Reference $ref,
        private readonly ScalarFunction|bool $exact = false,
    ) {
        $this->sum = 0;
        $this->window = null;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        $entry = $row->get($this->ref);

        if (!$this->floatColumn && $entry->definition()->type() instanceof FloatType) {
            $this->floatColumn = true;
        }

        try {
            // @mago-ignore analysis:mixed-assignment
            $value = $entry->value();

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->runningSum ??= new RunningSum($context->calculator());
                $this->sum = $this->runningSum->add($this->sum, $value, $this->isExact($row, $context));
                $this->aggregated++;
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Sum error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        return new SumAccumulator($this->ref, $this->exact, $context);
    }

    public function apply(WindowContext $window): mixed
    {
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
     * @return Entry<?float>|Entry<?int>
     */
    /**
     * @return null|list<Reference> null when $exact is a ScalarFunction - its entries cannot be
     *                              statically enumerated, so spill column pruning must be disabled
     */
    public function references(): ?array
    {
        return $this->exact instanceof ScalarFunction ? null : [$this->ref];
    }

    public function result(EntryFactory $entryFactory): Entry
    {
        $ref = $this->ref->hasAlias() ? $this->ref : $this->ref->as($this->ref->to() . '_sum');

        if ($this->floatColumn) {
            return float_entry($ref->name(), (float) $this->sum);
        }

        if ($this->aggregated === 0) {
            return int_entry($ref->name(), null);
        }

        if (!is_float($this->sum)) {
            return int_entry($ref->name(), (int) $this->sum);
        }

        return float_entry($ref->name(), $this->sum);
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

    private function isExact(Row $row, FlowContext $context): bool
    {
        if (is_bool($this->exact)) {
            return $this->exact;
        }

        return (new Parameter($this->exact))->asBoolean($row, $context);
    }
}

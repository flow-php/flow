<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

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

use function Flow\ETL\DSL\float_entry;
use function Flow\ETL\DSL\int_entry;
use function is_numeric;

final class Sum implements AggregatingFunction, FrameAccumulating, WindowFunction
{
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
        try {
            $value = $row->valueOf($this->ref);

            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->sum = $this->add($this->sum, $value, $this->isExact($row, $context), $context);
            }
        } catch (InvalidArgumentException $e) {
            $context->functions()->invalidResult(new InvalidArgumentException('Sum error: ' . $e->getMessage()));
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

    /**
     * @param float|int|numeric-string $value
     */
    private function add(float|int $sum, float|int|string $value, bool $exact, FlowContext $context): float|int
    {
        if ($exact) {
            return $context->calculator()->add($sum, $value);
        }

        $result = $sum + $value;

        if (
            is_float($result)
            && floor($result) === $result
            && $result >= (float) PHP_INT_MIN
            && $result < (float) PHP_INT_MAX
        ) {
            return (int) $result;
        }

        return $result;
    }

    private function isExact(Row $row, FlowContext $context): bool
    {
        if (is_bool($this->exact)) {
            return $this->exact;
        }

        return (new Parameter($this->exact))->asBoolean($row, $context);
    }
}

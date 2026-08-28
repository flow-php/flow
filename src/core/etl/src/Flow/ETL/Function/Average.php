<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\Calculator\Rounding;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\EntryFactory;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\AverageAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;

use function Flow\ETL\DSL\float_entry;
use function is_numeric;

final class Average implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    private int $count;

    private float $sum;

    private ?Window $window;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale = 2,
        private readonly Rounding $rounding = Rounding::HALF_UP,
    ) {
        $this->window = null;
        $this->count = 0;
        $this->sum = 0;
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        try {
            /** @var mixed $value */
            $value = $row->valueOf($this->ref);

            if (is_numeric($value)) {
                // @mago-ignore analysis:possibly-invalid-argument
                $this->sum = $context->calculator()->add($this->sum, $value);
                $this->count++;
            }
        } catch (InvalidArgumentException $e) {
            throw new InvalidArgumentException('Average error: ' . $e->getMessage(), 0, $e);
        }
    }

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        return new AverageAccumulator($this->ref, $this->scale, $this->rounding, $context);
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
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function result(EntryFactory $entryFactory): Entry
    {
        $ref = $this->ref->hasAlias() ? $this->ref : $this->ref->as($this->ref->to() . '_avg');

        if (0 === $this->count) {
            return float_entry($ref->name(), null);
        }

        return float_entry(
            $ref->name(),
            (float) (new Calculator())->divide($this->sum, $this->count, $this->scale, $this->rounding),
        );
    }

    public function toString(): string
    {
        return 'average()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\Calculator\Rounding;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\AverageAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;
use function is_numeric;

final class Average implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    use ResolvesFromChildren;

    private int $count;

    private readonly string $outputName;

    private float $sum;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale = 2,
        private readonly Rounding $rounding = Rounding::HALF_UP,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_avg';
        $this->count = 0;
        $this->sum = 0;
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<Reference> $children */
        return new self($children[0], $this->scale, $this->rounding, $this->window);
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        /** @var mixed $value */
        $value = $row->get($this->ref);

        if (is_numeric($value)) {
            // @mago-ignore analysis:possibly-invalid-argument
            $this->sum = $context->calculator()->add($this->sum, $value);
            $this->count++;
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
        return new self($this->ref, $this->scale, $this->rounding, $window);
    }

    public function outputName(): string
    {
        return $this->outputName;
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function value(): ?float
    {
        if (0 === $this->count) {
            return null;
        }

        return (float) (new Calculator())->divide($this->sum, $this->count, $this->scale, $this->rounding);
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

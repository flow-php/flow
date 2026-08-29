<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\SumAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;
use function is_numeric;

final class Sum implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    use ResolvesFromChildren;

    private int $aggregated = 0;

    private readonly string $outputName;

    private ?RunningSum $runningSum = null;

    private float|int $sum;

    public function __construct(
        private readonly Reference $ref,
        private readonly ScalarFunction|bool $exact = false,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_sum';
        $this->sum = 0;
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return $this->exact instanceof ScalarFunction ? [$this->ref, $this->exact] : [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var array{0: Reference, 1?: ScalarFunction} $children */
        return new self($children[0], $children[1] ?? $this->exact, $this->window);
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        try {
            $value = $row->valueOf($this->ref);

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
        return new self($this->ref, $this->exact, $window);
    }

    /**
     * @return null|list<Reference> null when $exact is a ScalarFunction - its entries cannot be
     *                              statically enumerated, so spill column pruning must be disabled
     */
    public function references(): ?array
    {
        return $this->exact instanceof ScalarFunction ? null : [$this->ref];
    }

    public function outputName(): string
    {
        return $this->outputName;
    }

    /**
     * float, not the argument type - RunningSum::add() promotes to float on int overflow, so integer
     * would be a declaration the accumulator can violate on ordinary data (Spark promotes to
     * LongType/DoubleType the same way; flow has no wider integer).
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function value(): ?float
    {
        if ($this->aggregated === 0) {
            return null;
        }

        return (float) $this->sum;
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

        return (new Parameter($this->exact))->asBoolean($row, $context) ?? false;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\Calculator;
use Flow\Calculator\Rounding;
use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\AverageAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;
use function is_numeric;
use function sprintf;

final class Average implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    use ResolvesFromChildren;

    private int $count = 0;

    private readonly string $outputName;

    private ?RunningSum $runningSum = null;

    public function __construct(
        private readonly Reference $ref,
        private readonly int $scale = 2,
        private readonly Rounding $rounding = Rounding::HALF_UP,
        private readonly bool $exact = false,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_avg';
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
        return new self($children[0], $this->scale, $this->rounding, $this->exact, $this->window);
    }

    public function aggregate(Rows $rows, array $indices, FlowContext $context): void
    {
        if ($rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->base())->take($indices)->values() as $value) {
            if (is_numeric($value)) {
                $this->runningSum ??= new RunningSum($context->calculator());
                // @mago-ignore analysis:possibly-invalid-argument
                $this->runningSum->add($value, $this->exact);
                $this->count++;
            }
        }
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        if ($other->runningSum === null) {
            return;
        }

        $this->runningSum ??= new RunningSum($context->calculator());
        $this->runningSum->merge($other->runningSum, $this->exact);
        $this->count += $other->count;
    }

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        return new AverageAccumulator($this->ref, $this->scale, $this->rounding, $this->exact, $context);
    }

    public function apply(WindowContext $window): mixed
    {
        $accumulator = $this->accumulator($window->flowContext());

        $frame = $window->frame();

        for ($index = 0, $count = $frame->count(); $index < $count; $index++) {
            $accumulator->accumulate($frame, $index);
        }

        return $accumulator->value();
    }

    public function over(Window $window): static
    {
        return new self($this->ref, $this->scale, $this->rounding, $this->exact, $window);
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
        if (0 === $this->count || $this->runningSum === null) {
            return null;
        }

        return (new Calculator())->divide($this->runningSum->value(), $this->count, $this->scale, $this->rounding);
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

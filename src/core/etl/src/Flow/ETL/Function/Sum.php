<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\Calculator\RunningSum;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row\Reference;
use Flow\ETL\Rows;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\SumAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;
use function is_numeric;
use function sprintf;

final class Sum implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    use ResolvesFromChildren;

    private int $aggregated = 0;

    private readonly string $outputName;

    private ?RunningSum $runningSum = null;

    public function __construct(
        private readonly Reference $ref,
        private readonly bool $exact = false,
        private readonly ?Window $window = null,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_sum';
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
        /** @var array{0: Reference} $children */
        return new self($children[0], $this->exact, $this->window);
    }

    public function aggregate(Rows $rows, array $indices, FlowContext $context): void
    {
        if ($rows->schema()->findDefinition($this->ref->base()) === null) {
            return;
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->base())->take($indices)->values() as $value) {
            if (is_int($value) || is_float($value) || is_string($value) && is_numeric($value)) {
                $this->runningSum ??= new RunningSum($context->calculator());
                $this->runningSum->add($value, $this->exact);
                $this->aggregated++;
            }
        }
    }

    public function merge(AggregatingFunction $other, FlowContext $context): void
    {
        if (!$other instanceof self) {
            throw new InvalidArgumentException(sprintf('%s cannot merge %s', self::class, $other::class));
        }

        if ($other->aggregated === 0 || $other->runningSum === null) {
            return;
        }

        $this->runningSum ??= new RunningSum($context->calculator());
        $this->runningSum->merge($other->runningSum, $this->exact);
        $this->aggregated += $other->aggregated;
    }

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        return new SumAccumulator($this->ref, $this->exact, $context);
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
        return new self($this->ref, $this->exact, $window);
    }

    /**
     * @return list<Reference>
     */
    public function references(): array
    {
        return [$this->ref];
    }

    public function outputName(): string
    {
        return $this->outputName;
    }

    /**
     * float, not the argument type - RunningSum promotes to float on int overflow, so integer
     * would be a declaration the accumulator can violate on ordinary data
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function value(): ?float
    {
        if ($this->aggregated === 0 || $this->runningSum === null) {
            return null;
        }

        return (float) $this->runningSum->value();
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
}

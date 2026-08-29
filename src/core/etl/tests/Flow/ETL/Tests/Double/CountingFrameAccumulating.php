<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Double;

use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FrameAccumulating;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ResolvesFromChildren;
use Flow\ETL\Function\WindowFunction;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_optional;

/**
 * Sums the referenced entry over the frame, counting how many accumulators WindowProcessor builds and
 * how many rows it feeds them. Pins the incremental strategy: a partition of n rows must never cost more
 * than n accumulate() calls when the frame only grows.
 *
 * over() and withChildren() return copies, so every copy routes its counts back to the instance the
 * test holds.
 */
final class CountingFrameAccumulating implements FrameAccumulating, WindowFunction
{
    use ResolvesFromChildren;

    public int $accumulateCalls = 0;

    public int $accumulatorCalls = 0;

    public int $valueCalls = 0;

    public function __construct(
        private readonly Reference $ref,
        private readonly ?Window $window = null,
        private readonly ?self $countersOwner = null,
    ) {}

    public function accumulator(FlowContext $context): FrameAccumulator
    {
        $this->counters()->accumulatorCalls++;

        return new CountingFrameAccumulator($this->ref, $this->counters());
    }

    public function apply(WindowContext $window): mixed
    {
        $accumulator = $this->accumulator($window->flowContext());

        foreach ($window->frame() as $frameRow) {
            $accumulator->accumulate($frameRow);
        }

        return $accumulator->value();
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return [$this->ref];
    }

    public function counters(): self
    {
        return $this->countersOwner ?? $this;
    }

    public function over(Window $window): static
    {
        return new self($this->ref, $window, $this->counters());
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional(type_float());
    }

    public function toString(): string
    {
        return 'counting_frame_accumulating()';
    }

    public function window(): Window
    {
        if ($this->window === null) {
            throw new RuntimeException('Window function "' . $this->toString() . '" requires an OVER clause.');
        }

        return $this->window;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<Reference> $children */
        return new self($children[0], $this->window, $this->counters());
    }
}

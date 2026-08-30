<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\RuntimeException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Window;
use Flow\ETL\Window\Accumulator\CountAccumulator;
use Flow\ETL\Window\FrameAccumulator;
use Flow\ETL\Window\WindowContext;
use Flow\Types\Type;

use function Flow\Types\DSL\type_integer;

final class Count implements AggregatingFunction, FrameAccumulating, WindowFunction
{
    use ResolvesFromChildren;

    private int $count;

    private readonly string $outputName;

    public function __construct(
        private readonly ?Reference $ref = null,
        private readonly ?Window $window = null,
    ) {
        if ($ref === null) {
            $this->outputName = '_count';
        } else {
            $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_count';
        }
        $this->count = 0;
    }

    /**
     * @return list<FunctionTree>
     */
    public function children(): array
    {
        return $this->ref === null ? [] : [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        if ($children === []) {
            if ($this->ref !== null) {
                throw InvalidLogicException::because(
                    'count() has a reference child; withChildren() must receive it back.',
                );
            }

            return $this;
        }

        if ($this->ref === null) {
            throw InvalidLogicException::because(
                'count() has no reference child; withChildren() must receive an empty list.',
            );
        }

        /** @var list<Reference> $children */
        return new self($children[0], $this->window);
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
        return new self($this->ref, $window);
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
        return $this->ref === null ? [] : [$this->ref];
    }

    /**
     * NOT NULL - an empty group counts to 0, never to null.
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_integer();
    }

    public function value(): int
    {
        return $this->count;
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

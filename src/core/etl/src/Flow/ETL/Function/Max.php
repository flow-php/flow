<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use DateTimeInterface;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Types\Type;

use function Flow\Types\DSL\type_optional;
use function is_numeric;
use function max;

final class Max implements AggregatingFunction
{
    use ResolvesFromChildren;

    private float|DateTimeInterface|null $max;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_max';
        $this->max = null;
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
        return new self($children[0]);
    }

    public function aggregate(Row $row, FlowContext $context): void
    {
        if (!$row->has($this->ref)) {
            return;
        }

        /** @var mixed $value */
        $value = $row->get($this->ref);

        if ($this->max === null) {
            if (is_numeric($value)) {
                $this->max = (float) $value;
            } elseif ($value instanceof DateTimeInterface) {
                $this->max = $value;
            }
        } else {
            if (is_numeric($value)) {
                $this->max = max($this->max, (float) $value);
            } elseif ($value instanceof DateTimeInterface) {
                $this->max = max($this->max, $value);
            }
        }
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
     * Exactly the argument type, nullable - an all-null or ref-less group leaves no maximum
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->ref->returns());
    }

    public function value(): float|DateTimeInterface|null
    {
        return $this->max;
    }
}

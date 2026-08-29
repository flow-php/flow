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
use function min;

final class Min implements AggregatingFunction
{
    use ResolvesFromChildren;

    private float|DateTimeInterface|null $min;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_min';
        $this->min = null;
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
        $value = $row->valueOf($this->ref);

        if ($this->min === null) {
            if (is_numeric($value)) {
                $this->min = (float) $value;
            } elseif ($value instanceof DateTimeInterface) {
                $this->min = $value;
            }
        } else {
            if (is_numeric($value)) {
                $this->min = min($this->min, (float) $value);
            } elseif ($value instanceof DateTimeInterface) {
                $this->min = min($this->min, $value);
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
     * Exactly the argument type, nullable - an all-null or ref-less group leaves no minimum
     * (DuckDB first_last_any.cpp: return_type = arguments[0]->return_type).
     *
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->ref->returns());
    }

    public function value(): float|DateTimeInterface|null
    {
        return $this->min;
    }
}

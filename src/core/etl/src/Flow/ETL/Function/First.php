<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Types\Type;

use function Flow\Types\DSL\type_optional;

final class First implements AggregatingFunction
{
    use ResolvesFromChildren;

    /**
     * @var null|array<array-key, mixed>|bool|float|int|object|string
     */
    private mixed $first;

    private bool $found;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_first';
        $this->first = null;
        $this->found = false;
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

        if (!$this->found) {
            $this->first = $row->valueOf($this->ref);
            $this->found = true;
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
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return type_optional($this->ref->returns());
    }

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function value(): mixed
    {
        return $this->first;
    }
}

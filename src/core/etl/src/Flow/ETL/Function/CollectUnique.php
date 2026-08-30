<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Types\Type;

use function current;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_optional;
use function in_array;

final class CollectUnique implements AggregatingFunction
{
    use ResolvesFromChildren;

    /**
     * @var array<mixed>
     */
    private array $collection;

    private readonly string $outputName;

    public function __construct(
        private readonly Reference $ref,
    ) {
        $this->outputName = $ref->hasAlias() ? $ref->name() : $ref->to() . '_collection_unique';
        $this->collection = [];
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

        /** @var array<string, mixed> $values */
        $values = [];

        $values[$this->ref->name()] = $row->get($this->ref);

        /** @var mixed $value */
        $value = current($values);

        if (!in_array($value, $this->collection, true)) {
            $this->collection[] = $value;
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
        return type_optional(type_list($this->ref->returns()));
    }

    /**
     * @return array<mixed>
     */
    public function value(): array
    {
        return $this->collection;
    }
}

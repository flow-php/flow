<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Column\Column;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ListFunctions;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Function\StructureFunctions;
use Flow\ETL\Rows;
use Flow\Types\Type;

final class ResolvedReference implements Reference
{
    use ScalarFunctionChain;

    /**
     * The Type is a constructor parameter so an untyped resolved node is unconstructable.
     * Nullability arrives fused into the Type as a top-level OptionalType, wrapped by UnresolvedReference::resolve().
     *
     * @param Type<mixed> $type
     */
    public function __construct(
        private readonly string $entry,
        private readonly Type $type,
        private readonly ?string $alias = null,
        private readonly SortOrder $sort = SortOrder::ASC,
        private readonly NullsOrder $nulls = NullsOrder::FIRST,
    ) {}

    public function __toString(): string
    {
        return $this->name();
    }

    public function as(string $alias): self
    {
        return new self($this->entry, $this->type, $alias, $this->sort, $this->nulls);
    }

    public function asc(NullsOrder $nulls = NullsOrder::FIRST): self
    {
        return new self($this->entry, $this->type, $this->alias, SortOrder::ASC, $nulls);
    }

    public function base(): string
    {
        return $this->entry;
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    public function desc(NullsOrder $nulls = NullsOrder::LAST): self
    {
        return new self($this->entry, $this->type, $this->alias, SortOrder::DESC, $nulls);
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        return $rows->column($this->base());
    }

    public function hasAlias(): bool
    {
        return $this->alias !== null;
    }

    public function is(Reference $ref): bool
    {
        return $this->name() === $ref->name();
    }

    public function list(): ListFunctions
    {
        return new ListFunctions($this);
    }

    public function name(): string
    {
        return $this->alias ?? $this->entry;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function nulls(): NullsOrder
    {
        return $this->nulls;
    }

    public function sort(): SortOrder
    {
        return $this->sort;
    }

    public function structure(): StructureFunctions
    {
        return new StructureFunctions($this);
    }

    public function to(): string
    {
        return $this->entry;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Reference;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Functions\FunctionTree;
use Flow\ETL\Functions\ListFunctions;
use Flow\ETL\Functions\ScalarFunction;
use Flow\ETL\Functions\ScalarFunctionChain;
use Flow\ETL\Functions\StructureFunctions;
use Flow\ETL\Kernels\Sort\SortColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\NullsOrder;
use Flow\ETL\Values\Rows;
use Flow\ETL\Values\SortOrder;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_from_array;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;

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
        private readonly string $column,
        private readonly Type $type,
        private readonly ?string $alias = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['column'] ?? null),
            type_from_array(type_map(type_string(), type_mixed())->assert($data['type'] ?? null)),
            type_optional(type_string())->assert($data['alias'] ?? null),
        );
    }

    public function __toString(): string
    {
        return $this->name();
    }

    public function as(string $alias): self
    {
        return new self($this->column, $this->type, $alias);
    }

    public function asc(NullsOrder $nulls = NullsOrder::FIRST): SortColumn
    {
        return new SortColumn($this->column, SortOrder::ASC, $nulls);
    }

    public function base(): string
    {
        return $this->column;
    }

    public function describe(): ?Description
    {
        return new Description(self::class, [
            'column' => $this->column,
            'type' => $this->type->normalize(),
            'alias' => $this->alias,
        ]);
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

    public function desc(NullsOrder $nulls = NullsOrder::LAST): SortColumn
    {
        return new SortColumn($this->column, SortOrder::DESC, $nulls);
    }

    public function eval(Rows $rows, Backend $backend): Column
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
        return $this->alias ?? $this->column;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        return $this->type;
    }

    public function structure(): StructureFunctions
    {
        return new StructureFunctions($this);
    }

    public function to(): string
    {
        return $this->column;
    }
}

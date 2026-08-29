<?php

declare(strict_types=1);

namespace Flow\ETL\Row;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\FunctionTree;
use Flow\ETL\Function\ListFunctions;
use Flow\ETL\Function\ScalarFunction;
use Flow\ETL\Function\ScalarFunctionChain;
use Flow\ETL\Function\StructureFunctions;
use Flow\ETL\Row;
use Flow\ETL\Schema\Definition;
use Flow\Types\Type;

use function Flow\Types\DSL\type_optional;
use function is_string;

final class UnresolvedReference implements Reference
{
    use ScalarFunctionChain;

    private const string UNRESOLVED =
        'Invalid call to %s() on unresolved reference "%s". The function tree must '
            . 'be resolved against a Schema before it can describe itself.';

    public function __construct(
        private readonly string $entry,
        private readonly ?string $alias = null,
        private readonly SortOrder $sort = SortOrder::ASC,
    ) {}

    public static function init(string|Reference $ref): Reference
    {
        if (is_string($ref)) {
            return new self($ref);
        }

        return $ref;
    }

    public function __toString(): string
    {
        return $this->name();
    }

    public function as(string $alias): self
    {
        return new self($this->entry, $alias, $this->sort);
    }

    public function asc(): self
    {
        return new self($this->entry, $this->alias, SortOrder::ASC);
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

    public function resolved(): bool
    {
        return false;
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        return $this;
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        throw InvalidLogicException::because(self::UNRESOLVED, 'returns', $this->entry);
    }

    public function desc(): self
    {
        return new self($this->entry, $this->alias, SortOrder::DESC);
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        return $row->valueOf($this->entry);
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
     * Kept on the leaf rather than inlined in the resolver so $alias/$sort copying stays in the class
     * that owns those fields - the resolver never learns the leaf's internal shape.
     *
     * The Definition's type and nullability are fused here, at the resolver boundary. This is also the
     * one place the fusion can fail - type_optional() refuses a multi-member UnionType, so a nullable
     * UnionDefinition has no expressible returns(). Doing it here is what keeps
     * ResolvedReference::returns() total.
     */
    public function resolve(Definition $definition): ResolvedReference
    {
        return new ResolvedReference(
            $this->entry,
            $definition->isNullable() ? type_optional($definition->type()) : $definition->type(),
            $this->alias,
            $this->sort,
        );
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

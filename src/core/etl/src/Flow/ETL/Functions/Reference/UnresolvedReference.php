<?php

declare(strict_types=1);

namespace Flow\ETL\Functions\Reference;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
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
use Flow\ETL\Values\Schema\Definition;
use Flow\ETL\Values\SortOrder;
use Flow\Types\Type;
use SensitiveParameter;

use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function is_string;

final class UnresolvedReference implements Reference
{
    use ScalarFunctionChain;

    private const string UNRESOLVED =
        'Invalid call to %s() on unresolved reference "%s". The function tree must '
            . 'be resolved against a Schema before it can describe itself.';

    public function __construct(
        private readonly string $column,
        private readonly ?string $alias = null,
    ) {}

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self(
            type_string()->assert($data['column'] ?? null),
            type_optional(type_string())->assert($data['alias'] ?? null),
        );
    }

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
        return new self($this->column, $alias);
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
        throw InvalidLogicException::because(self::UNRESOLVED, 'returns', $this->column);
    }

    public function desc(NullsOrder $nulls = NullsOrder::LAST): SortColumn
    {
        return new SortColumn($this->column, SortOrder::DESC, $nulls);
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        throw InvalidLogicException::because(self::UNRESOLVED, 'eval', $this->column);
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
     * Kept on the leaf rather than inlined in the resolver so $alias/$sort copying stays in the class
     * that owns those fields - the resolver never learns the leaf's internal shape.
     *
     * The Definition's type and nullability are fused here, at the resolver boundary.
     */
    public function resolve(Definition $definition): ResolvedReference
    {
        $type = $definition->type();

        return new ResolvedReference(
            $this->column,
            $definition->isNullable() ? type_optional($type) : $type,
            $this->alias,
        );
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

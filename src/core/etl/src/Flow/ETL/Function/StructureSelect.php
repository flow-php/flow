<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\StructureEntry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\UnresolvedReference;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final readonly class StructureSelect implements ScalarFunction
{
    use ResolvesFromChildren;

    private Reference $ref;

    private References $refs;

    public function __construct(Reference|string $ref, Reference|string ...$refs)
    {
        $this->ref = UnresolvedReference::init($ref);
        $this->refs = References::init(...$refs);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        /** @var list<ScalarFunction> UnresolvedReference and ResolvedReference are both scalar functions */
        return [$this->ref, ...$this->refs->all()];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        foreach ($children as $child) {
            if (!$child instanceof Reference) {
                throw InvalidLogicException::because(
                    'StructureSelect child must be a Reference, got "%s".',
                    $child::class,
                );
            }
        }

        /** @var non-empty-list<Reference> $children */
        return new self(...$children);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        /** @var ScalarFunction $ref both reference implementations are scalar functions */
        $ref = $this->ref;
        $structure = type_bare($ref->returns());

        if (!$structure instanceof StructureType) {
            throw SchemaNotDerivableException::function(
                'structure_select',
                'the structure operand declares "' . $structure->toString() . '", which is not a structure',
            );
        }

        $all = $structure->elements() + $structure->optionalElements();
        $elements = [];

        foreach ($this->refs as $selected) {
            $elements[$selected->name()] = $all[$selected->to()] ?? type_null();
        }

        return type_optional(type_structure($elements));
    }

    /**
     * @return null|array<string, mixed>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        if (!$row->has($this->ref)) {
            return null;
        }

        $structure = $row->get($this->ref);

        if (!$structure instanceof StructureEntry) {
            return null;
        }

        $value = $structure->value();
        $output = [];

        foreach ($this->refs as $ref) {
            if ($value !== null && array_key_exists($ref->to(), $value)) {
                $output[$ref->name()] = $value[$ref->to()];
            } else {
                $output[$ref->name()] = null;
            }
        }

        return $output;
    }
}

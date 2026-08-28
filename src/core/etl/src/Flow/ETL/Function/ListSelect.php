<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry\ListEntry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\UnresolvedReference;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;
use function is_array;

final readonly class ListSelect implements ScalarFunction
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
                throw InvalidLogicException::because('ListSelect child must be a Reference, got "%s".', $child::class);
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
        $list = type_bare($ref->returns());

        if (!$list instanceof ListType) {
            throw SchemaNotDerivableException::function(
                'list_select',
                'the list operand declares "' . $list->toString() . '", which is not a list',
            );
        }

        $element = type_bare($list->element());
        $elements = [];

        foreach ($this->refs as $selected) {
            $elements[$selected->name()] = match (true) {
                $element instanceof StructureType => (
                    $element->elements() + $element->optionalElements()
                )[$selected->to()] ?? type_null(),
                $element instanceof MapType => type_optional($element->value()),
                default => type_null(),
            };
        }

        return type_optional(type_list(type_structure($elements)));
    }

    /**
     * @return null|array<int, array<string, mixed>>
     */
    public function eval(Row $row, FlowContext $context): ?array
    {
        if (!$row->has($this->ref)) {
            return null;
        }

        $list = $row->get($this->ref);

        if (!$list instanceof ListEntry) {
            return null;
        }

        $output = [];

        foreach ($list->value() ?: [] as $index => $element) {
            $output[$index] = [];

            foreach ($this->refs as $ref) {
                if (is_array($element) && array_key_exists($ref->to(), $element)) {
                    $output[$index][$ref->name()] = $element[$ref->to()];
                } else {
                    $output[$index][$ref->name()] = null;
                }
            }
        }

        return $output;
    }
}

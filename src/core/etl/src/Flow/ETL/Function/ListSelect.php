<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\UnresolvedReference;
use Flow\Types\Type;
use Flow\Types\Type\ArrayKey;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function array_key_exists;
use function array_values;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
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
        return [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        if (!$children[0] instanceof Reference) {
            throw InvalidLogicException::because(
                'ListSelect child must be a Reference, got "%s".',
                $children[0]::class,
            );
        }

        return new self($children[0], ...$this->refs->all());
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $list = type_bare($this->ref->returns());

        if (!$list instanceof ListType) {
            throw SchemaNotDerivableException::function(
                'list_select',
                'the list operand declares "' . $list->toString() . '", which is not a list',
            );
        }

        $element = type_bare($list->element());
        $elements = [];

        foreach ($this->refs as $selected) {
            if ($element instanceof StructureType) {
                $selectedElement = $element->element(ArrayKey::coerce($selected->to()));

                $elements[] = $selectedElement === null
                    ? structure_element($selected->name(), type_null())
                    : structure_element($selected->name(), $selectedElement->type, $selectedElement->optional);

                continue;
            }

            $elements[] = $element instanceof MapType
                ? structure_element($selected->name(), type_optional($element->value()))
                : structure_element($selected->name(), type_null());
        }

        return type_optional(type_list(new StructureType($elements)));
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

        if (!is_array($list)) {
            return null;
        }

        $output = [];

        // @mago-ignore analysis:mixed-assignment
        foreach (array_values($list) as $index => $element) {
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

<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Row\Reference;
use Flow\ETL\Row\References;
use Flow\ETL\Row\UnresolvedReference;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\ArrayKey;
use Flow\Types\Type\Logical\StructureType;

use function array_fill;
use function array_key_exists;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_null;
use function Flow\Types\DSL\type_optional;
use function is_array;

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
        // The selected refs index INTO the structure's fields, never the row schema - returns() reads them as
        // names, so the outer resolver must not touch them (the Exists::$ref exclusion, one level down).
        return [$this->ref];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        if (!$children[0] instanceof Reference) {
            throw InvalidLogicException::because(
                'StructureSelect child must be a Reference, got "%s".',
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
        $structure = type_bare($this->ref->returns());

        if (!$structure instanceof StructureType) {
            throw SchemaNotDerivableException::function(
                'structure_select',
                'the structure operand declares "' . $structure->toString() . '", which is not a structure',
            );
        }

        $elements = [];

        foreach ($this->refs as $selected) {
            $selectedElement = $structure->element(ArrayKey::coerce($selected->to()));

            $elements[] = $selectedElement === null
                ? structure_element($selected->name(), type_null())
                : structure_element($selected->name(), $selectedElement->type, $selectedElement->optional);
        }

        return type_optional(new StructureType($elements));
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        if ($rows->schema()->findDefinition($this->ref->base()) === null) {
            // @mago-ignore analysis:possibly-invalid-argument
            return (new ResultColumn($context->backend()))->of(
                $this,
                $rows->isEmpty() ? [] : array_fill(0, $rows->count(), null),
            );
        }

        $results = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($rows->column($this->ref->base())->values() as $value) {
            if (!is_array($value)) {
                $results[] = null;

                continue;
            }

            $output = [];

            foreach ($this->refs as $ref) {
                if (array_key_exists($ref->to(), $value)) {
                    $output[$ref->name()] = $value[$ref->to()];
                } else {
                    $output[$ref->name()] = null;
                }
            }

            $results[] = $output;
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}

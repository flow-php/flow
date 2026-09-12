<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\InvalidLogicException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function array_combine;
use function array_is_list;
use function array_keys;
use function array_values;
use function count;
use function Flow\Types\DSL\structure_element;

final class Structure implements ScalarFunction
{
    use ScalarFunctionChain;

    /**
     * @var non-empty-array<array-key, ScalarFunction>
     */
    private readonly array $elements;

    /**
     * @param array<array-key, ScalarFunction> $elements
     *
     * @throws InvalidArgumentException
     */
    public function __construct(array $elements)
    {
        if ($elements === []) {
            throw InvalidArgumentException::because('Structure requires at least one element.');
        }

        // a value keyed 0..n-1 is a list, and a list is never a valid structure value
        if (array_is_list($elements)) {
            throw InvalidArgumentException::because(
                'Structure keys cannot be a list (0, 1, 2, ...). Name the elements or use non-sequential keys.',
            );
        }

        $this->elements = $elements;
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return array_values($this->elements);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        if (count($children) !== count($this->elements)) {
            throw InvalidLogicException::because(
                'Structure expects %d children, got %d.',
                count($this->elements),
                count($children),
            );
        }

        /** @var list<ScalarFunction> $children */
        return new self(array_combine(array_keys($this->elements), $children));
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $elements = [];

        foreach ($this->elements as $name => $element) {
            $elements[] = structure_element($name, $element->returns());
        }

        return new StructureType($elements);
    }

    /**
     * @return non-empty-array<array-key, mixed>
     */
    public function eval(Row $row, FlowContext $context): array
    {
        $values = [];

        foreach ($this->elements as $name => $element) {
            $values[$name] = $element->eval($row, $context);
        }

        return $values;
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\InvalidLogicException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

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

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->all($data['elements'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, ['elements' => $this->elements]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $elements = [];

        foreach ($this->elements as $name => $element) {
            $elements[$name] = (new Parameter($element))->values($rows, $backend);
        }

        $results = [];

        for ($i = 0, $count = $rows->count(); $i < $count; $i++) {
            $values = [];

            foreach ($elements as $name => $element) {
                $values[$name] = $element[$i];
            }

            $results[] = $values;
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}

<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_list;
use function is_array;

final class ArrayValues implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(ScalarFunction|array $array)
    {
        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->array->returns());

        return match (true) {
            $array instanceof ListType => type_list($array->element()),
            $array instanceof MapType => type_list($array->value()),
            $array instanceof StructureType => type_list(StructureValues::type('array_values', $array)),
            default => throw SchemaNotDerivableException::function(
                'array_values',
                'the array operand declares "' . $array->toString() . '", which has no element type',
            ),
        };
    }

    /**
     * @return null|array<int, mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);

        if (!is_array($array)) {
            throw new InvalidArgumentException('ArrayValues function requires non-null array');
        }

        return array_values($array);
    }
}

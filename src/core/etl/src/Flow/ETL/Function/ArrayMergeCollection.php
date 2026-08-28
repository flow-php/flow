<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;

use function array_merge;
use function array_values;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function is_array;

final class ArrayMergeCollection implements ScalarFunction
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

        if ($array instanceof ListType && type_bare($array->element()) instanceof ListType) {
            return type_bare($array->element());
        }

        throw SchemaNotDerivableException::function(
            'array_merge_collection',
            'the array operand declares "' . $array->toString() . '", which is not a list of lists',
        );
    }

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->array))->asArray($row, $context);

        if ($array === null) {
            throw new InvalidArgumentException('ArrayMergeCollection function requires non-null array');
        }

        // @mago-ignore analysis:mixed-assignment
        foreach ($array as $element) {
            if (!is_array($element)) {
                throw new InvalidArgumentException(
                    'ArrayMergeCollection function requires array elements to be arrays',
                );
            }
        }

        /** @var array<array<mixed>> $array */
        return array_merge(...array_values($array));
    }
}

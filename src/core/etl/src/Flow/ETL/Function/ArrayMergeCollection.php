<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Exception\SchemaNotDerivableException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\StructureType;

use function array_merge;
use function array_values;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\structure_element;
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

        if ($array instanceof ListType) {
            $element = type_bare($array->element());

            if ($element instanceof ListType) {
                return $element;
            }

            // Merging zero structures yields {} - a field is present iff the collection is non-empty,
            // so every field of the merged structure is optional.
            if ($element instanceof StructureType) {
                $fields = [];

                foreach ($element->elements() as $structureElement) {
                    $fields[] = structure_element($structureElement->name, $structureElement->type, optional: true);
                }

                return new StructureType($fields);
            }
        }

        throw SchemaNotDerivableException::function(
            'array_merge_collection',
            'the array operand declares "' . $array->toString() . '", which is not a list of lists or structures',
        );
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $arrays = (new Parameter($this->array))->asArrays($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
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
                $results[] = array_merge(...array_values($array));
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }
}

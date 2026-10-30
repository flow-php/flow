<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Exception;
use Flow\ETL\Foundation\Describe\Description;
use Flow\ETL\Foundation\Describe\Rebuild;
use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Foundation\Exception\SchemaNotDerivableException;
use Flow\ETL\Functions\Evaluation\ResultColumn;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_merge;
use function array_values;
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
        $this->array = $array instanceof ScalarFunction ? $array : new Literal($array);
    }

    public static function fromDescription(#[SensitiveParameter] array $data, Rebuild $rebuild): static
    {
        return new self($rebuild->of($data['array'] ?? null, ScalarFunction::class));
    }

    public function describe(): ?Description
    {
        return Description::compose(self::class, [
            'array' => $this->array,
        ]);
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

    public function eval(Rows $rows, Backend $backend): Column
    {
        $arrays = (new Parameter($this->array))->asArrays($rows, $backend);
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

        return (new ResultColumn($backend))->of($this, $results);
    }
}

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
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\StructureType;
use SensitiveParameter;

use function array_keys;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_string;
use function is_array;

final class ArrayKeys implements ScalarFunction
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

        return match (true) {
            $array instanceof StructureType => type_list(type_string()),
            $array instanceof ListType => type_list(type_integer()),
            $array instanceof MapType => type_list($array->key()),
            default => throw SchemaNotDerivableException::function(
                'array_keys',
                'the array operand declares "' . $array->toString() . '", which has no key type',
            ),
        };
    }

    public function eval(Rows $rows, Backend $backend): Column
    {
        $arrays = (new Parameter($this->array))->asArrays($rows, $backend);
        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
                if (!is_array($array)) {
                    throw new InvalidArgumentException('ArrayKeys function requires non-null array');
                }

                $results[] = array_keys($array);
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($backend))->of($this, $results);
    }
}

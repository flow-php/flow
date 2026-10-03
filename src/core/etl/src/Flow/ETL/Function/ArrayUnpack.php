<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Function\ScalarFunction\UnpackResults;
use Flow\ETL\Rows;
use Flow\ETL\Schema;
use Flow\Types\Type;

use function count;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_structure;

final class ArrayUnpack implements ScalarFunction, UnpackResults
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $array;

    /**
     * @param array<array-key, mixed>|ScalarFunction $array
     */
    public function __construct(
        ScalarFunction|array $array,
        private readonly Schema $schema,
    ) {
        if (!count($schema)) {
            throw new InvalidArgumentException('array_unpack() requires at least one declared column');
        }

        $this->array = $array instanceof ScalarFunction ? $array : lit($array);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->array];
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $results = [];
        $i = 0;

        try {
            foreach ((new Parameter($this->array))->asArrays($rows, $context) as $i => $array) {
                $results[] = $array ?? throw new InvalidArgumentException('array_unpack() requires a non-null array');
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $elements = [];

        // every declared column is nullable - a payload that omits a key still has to produce it
        foreach ($this->schema->definitions() as $definition) {
            $name = $definition->entry()->name();
            $elements[$name] = structure_element($name, type_optional($definition->type()));
        }

        return type_structure($elements);
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->schema);
    }
}

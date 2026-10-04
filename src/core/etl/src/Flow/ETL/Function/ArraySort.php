<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Exception;
use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Function\Evaluation\ResultColumn;
use Flow\ETL\Rows;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function array_values;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function is_array;
use function krsort;
use function ksort;

final class ArraySort implements ScalarFunction
{
    use ScalarFunctionChain;

    private readonly ScalarFunction $flags;
    private readonly ScalarFunction $recursive;

    public function __construct(
        private readonly ScalarFunction $ref,
        private readonly Sort $sortFunction,
        ScalarFunction|int|null $flags,
        ScalarFunction|bool $recursive,
    ) {
        $this->flags = $flags instanceof ScalarFunction ? $flags : lit($flags);
        $this->recursive = $recursive instanceof ScalarFunction ? $recursive : lit($recursive);
    }

    /**
     * @return list<ScalarFunction>
     */
    public function children(): array
    {
        return [$this->ref, $this->flags, $this->recursive];
    }

    /**
     * @param list<FunctionTree> $children
     */
    public function withChildren(array $children): static
    {
        /** @var list<ScalarFunction> $children */
        return new self($children[0], $this->sortFunction, $children[1], $children[2]);
    }

    /**
     * @return Type<mixed>
     */
    public function returns(): Type
    {
        $array = type_bare($this->ref->returns());

        // Only a key sort reorders a structure's fields deterministically at bind time; a value sort's
        // field order is data-dependent, so the operand's declared order stands. Sorting by array key
        // rather than by a string comparator reproduces PHP's key order for integer element names.
        if (
            $array instanceof StructureType
            && ($this->sortFunction === Sort::ksort || $this->sortFunction === Sort::krsort)
        ) {
            $byName = [];

            foreach ($array->elements() as $element) {
                $byName[$element->name] = $element;
            }

            if ($this->sortFunction === Sort::ksort) {
                ksort($byName);
            } else {
                krsort($byName);
            }

            return new StructureType(array_values($byName));
        }

        return $array;
    }

    public function eval(Rows $rows, FlowContext $context): Column
    {
        $arrays = (new Parameter($this->ref))->asArrays($rows, $context);
        $flagsList = (new Parameter($this->flags))->asInts($rows, $context);
        $recursives = (new Parameter($this->recursive))->asBooleans($rows, $context);
        $results = [];
        $i = 0;

        try {
            foreach ($arrays as $i => $array) {
                $flags = $flagsList[$i];
                $recursive = $recursives[$i];

                $recursive ??= false;

                if ($array === null) {
                    throw new InvalidArgumentException('ArraySort function requires non-null array');
                }

                $this->recursiveSort($array, $this->sortFunction->value, $flags, $recursive);

                $results[] = $array;
            }
        } catch (Exception $e) {
            throw EvaluationException::at($i, $e);
        }

        return (new ResultColumn($context->backend()))->of($this, $results);
    }

    /**
     * @param array<array-key, mixed> $array
     */
    private function recursiveSort(array &$array, callable $function, ?int $flags, bool $recursive): void
    {
        /** @var mixed $value */
        foreach ($array as &$value) {
            if ($recursive && is_array($value)) {
                $this->recursiveSort($value, $function, $flags, true);
            }
        }

        if (null !== $flags) {
            $function($array, $flags);
        } else {
            $function($array);
        }
    }
}

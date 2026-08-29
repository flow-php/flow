<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Function\ArraySort\Sort;
use Flow\ETL\Row;
use Flow\Types\Type;
use Flow\Types\Type\Logical\StructureType;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_structure;
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
        // field order is data-dependent, so the operand's declared order stands. Field order is
        // bucket-scoped: StructureType keeps required fields before optional ones, so a mixed
        // structure sorts within each bucket, not across them.
        if (
            $array instanceof StructureType
            && ($this->sortFunction === Sort::ksort || $this->sortFunction === Sort::krsort)
        ) {
            $elements = $array->elements();
            $optionalElements = $array->optionalElements();

            if ($this->sortFunction === Sort::ksort) {
                ksort($elements);
                ksort($optionalElements);
            } else {
                krsort($elements);
                krsort($optionalElements);
            }

            return type_structure($elements, $optionalElements);
        }

        return $array;
    }

    /**
     * @return null|array<mixed>
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        $array = (new Parameter($this->ref))->asArray($row, $context);
        $flags = (new Parameter($this->flags))->asInt($row, $context);
        $recursive = (new Parameter($this->recursive))->asBoolean($row, $context) ?? false;

        if ($array === null) {
            throw new InvalidArgumentException('ArraySort function requires non-null array');
        }

        $this->recursiveSort($array, $this->sortFunction->value, $flags, $recursive);

        return $array;
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

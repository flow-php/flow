<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Column\Column;
use Flow\ETL\Exception\EvaluationException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Value\Json;
use UnitEnum;

use function array_map;
use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_bare;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_string;
use function get_debug_type;
use function implode;
use function is_float;
use function is_int;
use function is_numeric;
use function is_scalar;
use function sprintf;

/**
 * Every as*() reader separates the two axes: a genuine NULL flowing through is not an error and
 * propagates (or takes the reader's null-input default), while a non-null value that cannot coerce
 * throws, naming its row - optional() is the door for pipelines that want the old silent tolerance.
 */
final readonly class Parameter
{
    private ScalarFunction $function;

    public function __construct(mixed $function)
    {
        $this->function = $function instanceof ScalarFunction ? $function : lit($function);
    }

    /**
     * @return list<?array<array-key, mixed>>
     */
    public function asArrays(Rows $rows, FlowContext $context): array
    {
        return $this->arraysOf($this->column($rows, $context));
    }

    /**
     * @return list<?array<array-key, mixed>>
     */
    public function arraysOf(Column $column): array
    {
        $type = type_array();
        $arrays = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->values() as $i => $value) {
            if ($value === null) {
                $arrays[] = null;

                continue;
            }

            if ($value instanceof Json) {
                $arrays[] = $value->toArray();

                continue;
            }

            if (!$type->isValid($value)) {
                throw new EvaluationException(
                    $i,
                    new InvalidArgumentException(InvalidTypeException::value($value, $type)->getMessage()),
                );
            }

            $arrays[] = $value;
        }

        return $arrays;
    }

    /**
     * @return list<?bool>
     */
    public function asBooleans(Rows $rows, FlowContext $context): array
    {
        $column = $this->column($rows, $context);

        if (type_bare($column->type()) instanceof BooleanType) {
            /** @var list<?bool> */
            return $column->physicals();
        }

        $booleans = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->values() as $i => $value) {
            if ($value === null) {
                $booleans[] = null;

                continue;
            }

            if (!is_scalar($value)) {
                throw new EvaluationException(
                    $i,
                    new InvalidArgumentException(sprintf('Expected type "boolean", got "%s".', get_debug_type($value))),
                );
            }

            $booleans[] = (bool) $value;
        }

        return $booleans;
    }

    /**
     * @template T of UnitEnum
     *
     * @param class-string<T> $enumClass
     *
     * @return list<?T>
     */
    public function asEnums(Rows $rows, FlowContext $context, string $enumClass): array
    {
        /** @var list<?T> */
        return $this->checked($this->values($rows, $context), type_instance_of($enumClass));
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $class
     *
     * @return list<?T>
     */
    public function asInstancesOf(Rows $rows, FlowContext $context, string $class): array
    {
        /** @var list<?T> */
        return $this->checked($this->values($rows, $context), type_instance_of($class));
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? list<?int> : list<int>)
     */
    public function asInts(Rows $rows, FlowContext $context, ?int $default = null): array
    {
        $column = $this->column($rows, $context);

        /** @var list<?int> $ints */
        $ints = type_bare($column->type()) instanceof IntegerType
            ? $column->physicals()
            : $this->checked($column->values(), type_integer());

        return $default === null ? $ints : array_map(static fn(?int $int): int => $int ?? $default, $ints);
    }

    /**
     * @param class-string $class
     *
     * @return list<?list<object>>
     */
    public function asListsOfObjects(Rows $rows, FlowContext $context, string $class): array
    {
        $objectType = type_instance_of($class);
        $lists = [];

        foreach ($this->asArrays($rows, $context) as $i => $array) {
            if ($array === null) {
                $lists[] = null;

                continue;
            }

            $objects = [];

            // @mago-ignore analysis:mixed-assignment
            foreach ($array as $item) {
                if (!$objectType->isValid($item)) {
                    throw new EvaluationException(
                        $i,
                        new InvalidArgumentException(InvalidTypeException::value($item, $objectType)->getMessage()),
                    );
                }

                $objects[] = $item;
            }

            $lists[] = $objects;
        }

        return $lists;
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? list<int|float|null> : list<int|float>)
     */
    public function asNumbers(Rows $rows, FlowContext $context, int|float|null $default = null): array
    {
        $column = $this->column($rows, $context);
        $type = type_bare($column->type());

        if ($type instanceof IntegerType || $type instanceof FloatType) {
            /** @var list<int|float|null> $numbers */
            $numbers = $column->physicals();

            return $default === null
                ? $numbers
                : array_map(static fn(int|float|null $number): int|float => $number ?? $default, $numbers);
        }

        $numbers = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($column->values() as $i => $value) {
            if ($value === null) {
                $numbers[] = $default;

                continue;
            }

            if (is_int($value) || is_float($value)) {
                $numbers[] = $value;

                continue;
            }

            if (!is_numeric($value)) {
                throw new EvaluationException(
                    $i,
                    new InvalidArgumentException(sprintf('Expected type "numeric", got "%s".', get_debug_type($value))),
                );
            }

            // numeric-string: prefer int if the value is integral, otherwise float.
            $numbers[] = (string) (int) $value === $value ? (int) $value : (float) $value;
        }

        return $numbers;
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? list<?string> : list<string>)
     */
    public function asStrings(Rows $rows, FlowContext $context, ?string $default = null): array
    {
        $column = $this->column($rows, $context);

        /** @var list<?string> $strings */
        $strings = type_bare($column->type()) instanceof StringType
            ? $column->physicals()
            : $this->checked($column->values(), type_string());

        return $default === null
            ? $strings
            : array_map(static fn(?string $string): string => $string ?? $default, $strings);
    }

    /**
     * @template T
     *
     * @param Type<T> ...$types
     *
     * @return list<?T>
     */
    public function asTypes(Rows $rows, FlowContext $context, Type ...$types): array
    {
        $accepted = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($rows, $context) as $i => $value) {
            if ($value === null) {
                $accepted[] = null;

                continue;
            }

            foreach ($types as $nextType) {
                if ($nextType->isValid($value)) {
                    $accepted[] = $value;

                    continue 2;
                }
            }

            throw new EvaluationException(
                $i,
                new InvalidArgumentException(sprintf(
                    'Expected one of "%s", got "%s".',
                    implode('", "', array_map(static fn(Type $type): string => $type->toString(), $types)),
                    get_debug_type($value),
                )),
            );
        }

        return $accepted;
    }

    /**
     * @param list<mixed> $values
     * @param Type<mixed> $type
     *
     * @return list<mixed>
     */
    public function checked(array $values, Type $type): array
    {
        // @mago-ignore analysis:mixed-assignment
        foreach ($values as $i => $value) {
            if ($value !== null && !$type->isValid($value)) {
                throw new EvaluationException(
                    $i,
                    new InvalidArgumentException(InvalidTypeException::value($value, $type)->getMessage()),
                );
            }
        }

        return $values;
    }

    public function column(Rows $rows, FlowContext $context): Column
    {
        return $this->function->eval($rows, $context);
    }

    /**
     * @return list<mixed>
     */
    public function values(Rows $rows, FlowContext $context): array
    {
        return $this->column($rows, $context)->values();
    }
}

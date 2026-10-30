<?php

declare(strict_types=1);

namespace Flow\ETL\Functions;

use Flow\ETL\Foundation\Exception\EvaluationException;
use Flow\ETL\Foundation\Exception\InvalidArgumentException;
use Flow\ETL\Values\Column\Backend;
use Flow\ETL\Values\Column\Column;
use Flow\ETL\Values\Rows;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Value\Json;
use UnitEnum;

use function array_map;
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
        $this->function = $function instanceof ScalarFunction ? $function : new Literal($function);
    }

    /**
     * @return list<?array<array-key, mixed>>
     */
    public function asArrays(Rows $rows, Backend $backend): array
    {
        return $this->arraysOf($this->function->eval($rows, $backend));
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
    public function asBooleans(Rows $rows, Backend $backend): array
    {
        $column = $this->function->eval($rows, $backend);

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
    public function asEnums(Rows $rows, Backend $backend, string $enumClass): array
    {
        /** @var list<?T> */
        return $this->checked($this->values($rows, $backend), type_instance_of($enumClass));
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $class
     *
     * @return list<?T>
     */
    public function asInstancesOf(Rows $rows, Backend $backend, string $class): array
    {
        /** @var list<?T> */
        return $this->checked($this->values($rows, $backend), type_instance_of($class));
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? list<?int> : list<int>)
     */
    public function asInts(Rows $rows, Backend $backend, ?int $default = null): array
    {
        $column = $this->function->eval($rows, $backend);

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
    public function asListsOfObjects(Rows $rows, Backend $backend, string $class): array
    {
        $objectType = type_instance_of($class);
        $lists = [];

        foreach ($this->asArrays($rows, $backend) as $i => $array) {
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
    public function asNumbers(Rows $rows, Backend $backend, int|float|null $default = null): array
    {
        $column = $this->function->eval($rows, $backend);
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
    public function asStrings(Rows $rows, Backend $backend, ?string $default = null): array
    {
        $column = $this->function->eval($rows, $backend);

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
    public function asTypes(Rows $rows, Backend $backend, Type ...$types): array
    {
        $accepted = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($this->values($rows, $backend) as $i => $value) {
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

    /**
     * @return list<mixed>
     */
    public function values(Rows $rows, Backend $backend): array
    {
        return $this->function->eval($rows, $backend)->values();
    }
}

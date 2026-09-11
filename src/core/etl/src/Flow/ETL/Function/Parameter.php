<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\FlowContext;
use Flow\ETL\Row;
use Flow\ETL\Row\Reference;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use UnitEnum;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_string;
use function implode;
use function sprintf;

/**
 * Every as*() arm separates the two axes: a genuine NULL flowing through is not an error and
 * propagates (or takes the arm's null-input default), while a non-null value that cannot coerce
 * throws - optional() is the door for pipelines that want the old silent tolerance.
 */
final readonly class Parameter
{
    private ScalarFunction $function;

    public function __construct(mixed $function)
    {
        $this->function = $function instanceof ScalarFunction ? $function : lit($function);
    }

    /**
     * @template T
     *
     * @param Type<T> ...$types
     *
     * @return null|T
     */
    public function as(Row $row, FlowContext $context, Type ...$types): mixed
    {
        $value = $this->eval($row, $context);

        if ($value === null) {
            return null;
        }

        foreach ($types as $nextType) {
            if ($nextType->isValid($value)) {
                return $value;
            }
        }

        throw new InvalidArgumentException(sprintf(
            'Expected one of "%s", got "%s".',
            implode('", "', array_map(static fn(Type $type): string => $type->toString(), $types)),
            get_debug_type($value),
        ));
    }

    /**
     * @return null|array<array-key, mixed>
     */
    public function asArray(Row $row, FlowContext $context): ?array
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return null;
        }

        if ($result instanceof Json) {
            return $result->toArray();
        }

        $type = type_array();

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    public function asBoolean(Row $row, FlowContext $context): ?bool
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return null;
        }

        if (!is_scalar($result)) {
            throw new InvalidArgumentException(sprintf('Expected type "boolean", got "%s".', get_debug_type($result)));
        }

        return (bool) $result;
    }

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function asValue(Row $row): mixed
    {
        if ($this->function instanceof Reference) {
            return $row->has($this->function) ? $row->get($this->function) : null;
        }

        return null;
    }

    /**
     * @template T of UnitEnum
     *
     * @param class-string<T> $enumClass
     *
     * @return null|T
     */
    public function asEnum(Row $row, FlowContext $context, string $enumClass): ?UnitEnum
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return null;
        }

        $type = type_instance_of($enumClass);

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    public function asFloat(Row $row, FlowContext $context): ?float
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return null;
        }

        $type = type_float();

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    /**
     * @template T of object
     *
     * @param class-string<T> $class
     *
     * @return null|T
     */
    public function asInstanceOf(Row $row, FlowContext $context, string $class): ?object
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return null;
        }

        $type = type_instance_of($class);

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? int|null : int)
     */
    public function asInt(Row $row, FlowContext $context, ?int $default = null): ?int
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return $default;
        }

        $type = type_integer();

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    /**
     * @param class-string $class
     *
     * @return null|array<object>
     */
    public function asListOfObjects(Row $row, FlowContext $context, string $class): ?array
    {
        $result = $this->asArray($row, $context);

        if ($result === null) {
            return null;
        }

        $objectType = type_instance_of($class);
        $objects = [];

        // @mago-ignore analysis:mixed-assignment
        foreach ($result as $item) {
            if (!$objectType->isValid($item)) {
                throw new InvalidArgumentException(InvalidTypeException::value($item, $objectType)->getMessage());
            }

            $objects[] = $item;
        }

        return $objects;
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? int|float|null : int|float)
     */
    public function asNumber(Row $row, FlowContext $context, int|float|null $default = null): int|float|null
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return $default;
        }

        if (!is_numeric($result)) {
            throw new InvalidArgumentException(sprintf('Expected type "numeric", got "%s".', get_debug_type($result)));
        }

        if (is_int($result) || is_float($result)) {
            return $result;
        }

        // numeric-string: prefer int if the value is integral, otherwise float.
        if ((string) (int) $result === $result) {
            return (int) $result;
        }

        return (float) $result;
    }

    public function asObject(Row $row, FlowContext $context): ?object
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return null;
        }

        $type = type_object();

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    /**
     * $default applies to a NULL input only - a malformed value always throws.
     *
     * @return ($default is null ? string|null : string)
     */
    public function asString(Row $row, FlowContext $context, ?string $default = null): ?string
    {
        $result = $this->eval($row, $context);

        if ($result === null) {
            return $default;
        }

        $type = type_string();

        if (!$type->isValid($result)) {
            throw new InvalidArgumentException(InvalidTypeException::value($result, $type)->getMessage());
        }

        return $result;
    }

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        // @mago-ignore analysis:mixed-return-statement
        return $this->function->eval($row, $context);
    }
}

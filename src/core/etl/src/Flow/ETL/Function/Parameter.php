<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\Types\Exception\InvalidTypeException;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use UnitEnum;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\get_type;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_float;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_integer;
use function Flow\Types\DSL\type_object;
use function Flow\Types\DSL\type_string;

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

        foreach ($types as $nextType) {
            if ($nextType->isValid($value)) {
                return $value;
            }
        }

        return null;
    }

    /**
     * @return null|array<array-key, mixed>
     */
    public function asArray(Row $row, FlowContext $context): ?array
    {
        $result = $this->eval($row, $context);

        if ($result instanceof Json) {
            return $result->toArray();
        }

        try {
            return type_array()->assert($result);
        } catch (InvalidTypeException) {
            return null;
        }
    }

    public function asBoolean(Row $row, FlowContext $context): bool
    {
        $result = $this->eval($row, $context);

        return is_scalar($result) && (bool) $result;
    }

    /**
     * @return null|Entry<mixed>
     */
    public function asEntry(Row $row): ?Entry
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
        try {
            return type_instance_of($enumClass)->assert($this->eval($row, $context));
        } catch (InvalidTypeException) {
            return null;
        }
    }

    public function asFloat(Row $row, FlowContext $context): ?float
    {
        try {
            return type_float()->assert($this->eval($row, $context));
        } catch (InvalidTypeException) {
            return null;
        }
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
        try {
            return type_instance_of($class)->assert($this->eval($row, $context));
        } catch (InvalidTypeException) {
            return null;
        }
    }

    /**
     * @phpstan-return ($default is null ? int|null : int)
     */
    public function asInt(Row $row, FlowContext $context, ?int $default = null): ?int
    {
        try {
            return type_integer()->assert($this->eval($row, $context));
        } catch (InvalidTypeException) {
            return $default;
        }
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
            try {
                $objects[] = $objectType->assert($item);
            } catch (InvalidTypeException) {
                return null;
            }
        }

        return $objects;
    }

    /**
     * @phpstan-return ($default is null ? int|float|null : int|float)
     */
    public function asNumber(Row $row, FlowContext $context, int|float|null $default = null): int|float|null
    {
        $result = $this->eval($row, $context);

        if (!is_numeric($result)) {
            return $default;
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
        try {
            return type_object()->assert($this->eval($row, $context));
        } catch (InvalidTypeException) {
            return null;
        }
    }

    /**
     * @phpstan-return ($default is null ? string|null : string)
     */
    public function asString(Row $row, FlowContext $context, ?string $default = null): ?string
    {
        try {
            return type_string()->assert($this->eval($row, $context));
        } catch (InvalidTypeException) {
            return $default;
        }
    }

    /**
     * @return Type<mixed>
     */
    public function asType(Row $row, FlowContext $context): Type
    {
        if ($this->function instanceof Reference) {
            return $row->get($this->function)->type();
        }

        return get_type($this->eval($row, $context));
    }

    /**
     * @return null|array<array-key, mixed>|bool|float|int|object|string
     */
    public function eval(Row $row, FlowContext $context): mixed
    {
        // @mago-ignore analysis:mixed-assignment
        $result = $this->function->eval($row, $context);

        // @mago-ignore analysis:mixed-return-statement
        return $result instanceof ScalarResult ? $result->value : $result;
    }
}

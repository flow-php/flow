<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use function Flow\ETL\DSL\lit;
use Flow\ETL\PHP\Type\Type;
use Flow\ETL\Row;

final readonly class Parameter
{
    private ScalarFunction $function;

    public function __construct(mixed $function)
    {
        $this->function = $function instanceof ScalarFunction ? $function : lit($function);
    }

    /**
     * @param Type<mixed> $type
     * @param Type<mixed> ...$types
     */
    public function as(Row $row, Type $type, Type ...$types) : mixed
    {
        $value = $this->function->eval($row);

        foreach (\array_merge([$type], $types) as $nextType) {
            if ($nextType->isValid($value)) {
                return $value;
            }
        }

        return null;
    }

    public function asArray(Row $row) : ?array
    {
        $result = $this->function->eval($row);

        return \is_array($result) ? $result : null;
    }

    public function asBoolean(Row $row) : bool
    {
        return (bool) $this->function->eval($row);
    }

    /**
     * @template T of \UnitEnum
     *
     * @param Row $row
     * @param class-string<T> $enumClass
     *
     * @return null|T
     */
    public function asEnum(Row $row, string $enumClass) : ?\UnitEnum
    {
        $result = $this->function->eval($row);

        return \is_a($result, $enumClass) ? $result : null;
    }

    public function asFloat(Row $row) : ?float
    {
        $result = $this->function->eval($row);

        return \is_float($result) ? $result : null;
    }

    /**
     * @template T of object
     *
     * @param Row $row
     * @param class-string<T> $class
     *
     * @return null|T
     */
    public function asInstanceOf(Row $row, string $class) : ?object
    {
        $result = $this->function->eval($row);

        return \is_a($result, $class) ? $result : null;
    }

    /**
     * @phpstan-return ($default is null ? int|null : int)
     */
    public function asInt(Row $row, ?int $default = null) : ?int
    {
        $result = $this->function->eval($row);

        return \is_int($result) ? $result : $default;
    }

    public function asListOfObjects(Row $row, string $class) : ?array
    {
        $result = $this->function->eval($row);

        if (!\is_array($result)) {
            return null;
        }

        foreach ($result as $item) {
            if (!\is_object($item) || !\is_a($item, $class)) {
                return null;
            }
        }

        return $result;
    }

    /**
     * @phpstan-return ($default is null ? int|float|null : int|float)
     */
    public function asNumber(Row $row, int|float|null $default = null) : int|float|null
    {
        $result = $this->function->eval($row);

        if (\is_string($result)) {
            return $default;
        }

        return \is_numeric($result) ? $result : $default;
    }

    public function asObject(Row $row) : ?object
    {
        $result = $this->function->eval($row);

        return \is_object($result) ? $result : null;
    }

    /**
     * @phpstan-return ($default is null ? string|null : string)
     */
    public function asString(Row $row, ?string $default = null) : ?string
    {
        $result = $this->function->eval($row);

        return \is_string($result) ? $result : $default;
    }

    public function eval(Row $row) : mixed
    {
        return $this->function->eval($row);
    }
}

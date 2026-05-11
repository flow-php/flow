<?php

declare(strict_types=1);

namespace Flow\ETL\Function;

use Flow\ETL\FlowContext;
use Flow\ETL\Function\ScalarFunction\ScalarResult;
use Flow\ETL\Row;
use Flow\ETL\Row\Entry;
use Flow\ETL\Row\Reference;
use Flow\ETL\Schema\Metadata;
use Flow\Types\Type;
use Flow\Types\Value\Json;
use UnitEnum;

use function Flow\ETL\DSL\lit;
use function Flow\Types\DSL\get_type;
use function Flow\Types\DSL\type_null;

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

        return \is_array($result) ? $result : null;
    }

    public function asBoolean(Row $row, FlowContext $context): bool
    {
        $result = $this->eval($row, $context);

        return \is_scalar($result) ? (bool) $result : false;
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
     * @param Row $row
     * @param class-string<T> $enumClass
     *
     * @return null|T
     */
    public function asEnum(Row $row, FlowContext $context, string $enumClass): ?\UnitEnum
    {
        $result = $this->eval($row, $context);

        return \is_object($result) && \is_a($result, $enumClass) ? $result : null;
    }

    public function asFloat(Row $row, FlowContext $context): ?float
    {
        $result = $this->eval($row, $context);

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
    public function asInstanceOf(Row $row, FlowContext $context, string $class): ?object
    {
        $result = $this->eval($row, $context);

        return \is_object($result) && \is_a($result, $class) ? $result : null;
    }

    /**
     * @phpstan-return ($default is null ? int|null : int)
     */
    public function asInt(Row $row, FlowContext $context, ?int $default = null): ?int
    {
        $result = $this->eval($row, $context);

        return \is_int($result) ? $result : $default;
    }

    /**
     * @return null|array<object>
     */
    public function asListOfObjects(Row $row, FlowContext $context, string $class): ?array
    {
        $result = $this->eval($row, $context);

        if (!\is_array($result)) {
            return null;
        }

        foreach ($result as $item) {
            if (!\is_object($item) || !\is_a($item, $class)) {
                return null;
            }
        }

        /** @phpstan-ignore return.type */
        return $result;
    }

    /**
     * @phpstan-return ($default is null ? int|float|null : int|float)
     */
    public function asNumber(Row $row, FlowContext $context, int|float|null $default = null): int|float|null
    {
        $result = $this->eval($row, $context);

        if (!\is_numeric($result)) {
            return $default;
        }

        return match (true) {
            \is_int($result), \is_float($result) => $result,
            $result == (int) $result => (int) $result,
            $result == (float) $result => (float) $result,
            default => $default,
        };
    }

    public function asObject(Row $row, FlowContext $context): ?object
    {
        $result = $this->eval($row, $context);

        return \is_object($result) ? $result : null;
    }

    /**
     * @phpstan-return ($default is null ? string|null : string)
     */
    public function asString(Row $row, FlowContext $context, ?string $default = null): ?string
    {
        $result = $this->eval($row, $context);

        return \is_string($result) ? $result : $default;
    }

    /**
     * @return Type<mixed>
     */
    public function asType(Row $row, FlowContext $context): Type
    {
        if ($this->function instanceof Reference) {
            if ($row->get($this->function)->definition()->metadata()->has(Metadata::FROM_NULL)) {
                return type_null();
            }

            return $row->get($this->function)->type();
        }

        $result = $this->eval($row, $context);

        return get_type($result);
    }

    public function eval(Row $row, FlowContext $context): mixed
    {
        $result = $this->function->eval($row, $context);

        if ($result instanceof ScalarResult) {
            return $result->value;
        }

        return $result;
    }
}

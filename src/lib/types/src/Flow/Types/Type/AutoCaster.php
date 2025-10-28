<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{get_type,
    type_boolean,
    type_date,
    type_datetime,
    type_float,
    type_integer};
use Flow\Types\Type\Native\String\StringTypeChecker;

final readonly class AutoCaster
{
    public function cast(mixed $value) : mixed
    {
        if (\is_string($value)) {
            return $this->castToString($value);
        }

        if (\is_array($value)) {
            return $this->castArray($value);
        }

        return $value;
    }

    /**
     * @param array<array-key, mixed> $value
     *
     * @return array<array-key, mixed>
     */
    private function castArray(array $value) : array
    {
        $valueTypes = [];

        foreach ($value as $item) {
            $valueType = get_type($item);
            $valueTypes[$valueType->toString()] = $valueType;
        }

        if (isset($valueTypes['integer'], $valueTypes['float']) && \count($valueTypes) === 2) {
            return \array_map(fn ($item) => type_float()->cast($item), $value);
        }

        return $value;
    }

    private function castToString(string $value) : mixed
    {
        $typeChecker = new StringTypeChecker($value);

        if ($typeChecker->isNull()) {
            return null;
        }

        if ($typeChecker->isInteger()) {
            return type_integer()->cast($value);
        }

        if ($typeChecker->isFloat()) {
            return type_float()->cast($value);
        }

        if ($typeChecker->isBoolean()) {
            return type_boolean()->cast($value);
        }

        if ($typeChecker->isDate()) {
            return type_date()->cast($value);
        }

        if ($typeChecker->isDateTime()) {
            return type_datetime()->cast($value);
        }

        return $value;
    }
}

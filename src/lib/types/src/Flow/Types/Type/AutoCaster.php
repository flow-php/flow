<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{get_type,
    type_boolean,
    type_date,
    type_datetime,
    type_float,
    type_integer,
    type_json,
    type_uuid};
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
        $keyTypes = [];
        $valueTypes = [];

        foreach ($value as $key => $item) {
            $keyType = get_type($key);
            $valueType = get_type($item);
            $keyTypes[$keyType->toString()] = $keyType;
            $valueTypes[$valueType->toString()] = $valueType;
        }

        if (isset($valueTypes['integer'], $valueTypes['float']) && \count($valueTypes) === 2) {
            $castedArray = [];

            foreach ($value as $key => $item) {
                $castedArray[$key] = type_float()->cast($item);
            }

            return $castedArray;
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

        if ($typeChecker->isJson()) {
            return type_json()->cast($value);
        }

        if ($typeChecker->isUuid()) {
            return type_uuid()->cast($value);
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

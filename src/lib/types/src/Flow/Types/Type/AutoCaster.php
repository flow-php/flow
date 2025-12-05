<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use function Flow\Types\DSL\{get_type, type_float};
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\String\StringTypeNarrower;

final readonly class AutoCaster
{
    public function cast(mixed $value): mixed
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
    private function castArray(array $value): array
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

    private function castToString(string $value): mixed
    {
        $narrowedType = (new StringTypeNarrower())->narrow($value);

        if ($narrowedType instanceof NullType) {
            return null;
        }

        return $narrowedType->cast($value);
    }
}

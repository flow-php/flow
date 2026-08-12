<?php

declare(strict_types=1);

namespace Flow\Types\Type;

use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\String\StringTypeNarrower;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Value\Json;

use function count;
use function Flow\Types\DSL\get_type;
use function Flow\Types\DSL\type_float;
use function is_array;
use function is_string;
use function trim;

final readonly class AutoCaster
{
    public function cast(mixed $value): mixed
    {
        if (is_string($value)) {
            return $this->castToString($value);
        }

        if ($value instanceof Json) {
            return $this->castArray($value->toArray());
        }

        if (is_array($value)) {
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

        // @mago-ignore analysis:mixed-assignment
        foreach ($value as $key => $item) {
            $keyType = get_type($key);
            $valueType = get_type($item);
            $keyTypes[$keyType->toString()] = $keyType;
            $valueTypes[$valueType->toString()] = $valueType;
        }

        if (isset($valueTypes['integer'], $valueTypes['float']) && count($valueTypes) === 2) {
            $castedArray = [];

            // @mago-ignore analysis:mixed-assignment
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

        if ($narrowedType instanceof StringType) {
            return $value;
        }

        // narrow() detects on the trimmed value, so cast must see the same input -
        // BooleanType::cast(' false ') misses its 'false' match and truthiness makes it true
        return $narrowedType->cast(trim($value));
    }
}

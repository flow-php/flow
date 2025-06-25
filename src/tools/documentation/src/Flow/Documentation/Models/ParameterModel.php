<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function Flow\Types\DSL\{type_array, type_boolean, type_string, type_structure};

final class ParameterModel
{
    /**
     * @param string $name
     * @param TypesModel $type
     * @param bool $isNullable
     * @param bool $isVariadic
     */
    public function __construct(
        public readonly string $name,
        public readonly TypesModel $type,
        public readonly bool $hasDefaultValue,
        public readonly bool $isNullable,
        public readonly bool $isVariadic,
    ) {
    }

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data) : self
    {
        $data = type_structure([
            'name' => type_string(),
            'type' => type_array(),
            'has_default_value' => type_boolean(),
            'is_nullable' => type_boolean(),
            'is_variadic' => type_boolean(),
        ])->assert($data);

        /** @phpstan-var array<array<string, mixed>> $type */
        $type = $data['type'];

        return new self(
            $data['name'],
            TypesModel::fromArray($type),
            $data['has_default_value'],
            $data['is_nullable'],
            $data['is_variadic'],
        );
    }

    public static function fromReflection(\ReflectionParameter $reflectionParameter) : self
    {
        try {
            $reflectionParameter->getDefaultValue();
            $hasDefaultValue = true;
        } catch (\Throwable $e) {
            $hasDefaultValue = false;
        }

        $reflectionType = $reflectionParameter->getType();

        if ($reflectionType === null) {
            throw new \InvalidArgumentException('ReflectionType must be instance of ReflectionNamedType');
        }

        return new self(
            $reflectionParameter->getName(),
            TypesModel::fromReflection($reflectionType),
            $hasDefaultValue,
            $reflectionParameter->allowsNull(),
            $reflectionParameter->isVariadic(),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'type' => $this->type->normalize(),
            'has_default_value' => $this->hasDefaultValue,
            'is_nullable' => $this->isNullable,
            'is_variadic' => $this->isVariadic,
        ];
    }
}

<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

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

    public static function fromArray(array $data) : self
    {
        return new self(
            $data['name'],
            TypesModel::fromArray($data['type']),
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

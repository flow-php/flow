<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

final class TypesModel
{
    /**
     * @param array<TypeModel> $types
     */
    public function __construct(
        public readonly array $types,
    ) {
    }

    public static function fromArray(array $data) : self
    {
        return new self(
            array_map(static fn (array $type) => TypeModel::fromArray($type), $data),
        );
    }

    public static function fromReflection(\ReflectionType $reflectionType) : self
    {
        $types = match ($reflectionType::class) {
            \ReflectionIntersectionType::class => array_map(fn (\ReflectionType $type) => TypeModel::fromReflection($type), $reflectionType->getTypes()),
            \ReflectionNamedType::class => [TypeModel::fromReflection($reflectionType)],
            \ReflectionUnionType::class => array_map(fn (\ReflectionType $type) => TypeModel::fromReflection($type), $reflectionType->getTypes()),
            default => [],
        };

        return new self(
            $types,
        );
    }

    public function normalize() : array
    {
        return array_map(fn (TypeModel $type) => $type->normalize(), $this->types);
    }

    public function toString() : string
    {
        $normalizedNames = [];

        foreach ($this->types as $type) {
            $normalizedNames[] = $type->name();
        }

        return \implode('|', $normalizedNames);
    }
}

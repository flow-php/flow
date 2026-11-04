<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use Roave\BetterReflection\Reflection\ReflectionIntersectionType;
use Roave\BetterReflection\Reflection\ReflectionNamedType;
use Roave\BetterReflection\Reflection\ReflectionType;
use Roave\BetterReflection\Reflection\ReflectionUnionType;

use function count;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function implode;

final readonly class TypesModel
{
    /**
     * @param array<TypeModel> $types
     */
    public function __construct(
        public array $types,
    ) {}

    /**
     * @param array<array<string, mixed>> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_list(type_map(type_string(), type_mixed()))->assert($data);

        return new self(array_map(TypeModel::fromArray(...), $data));
    }

    public static function fromReflection(ReflectionType $reflectionType): self
    {
        // Better Reflection reports ?Type as Type|null, PHP reports one nullable type.
        if ($reflectionType instanceof ReflectionUnionType && count($reflectionType->getTypes()) === 2) {
            foreach ($reflectionType->getTypes() as $type) {
                if (
                    $type instanceof ReflectionNamedType
                    && $type->getName() !== 'null'
                    && $reflectionType->allowsNull()
                ) {
                    $nullableType = TypeModel::fromReflection($type);

                    return new self([new TypeModel($nullableType->name, $nullableType->namespace, true, false)]);
                }
            }
        }

        $types = match ($reflectionType::class) {
            ReflectionIntersectionType::class => array_map(static fn(ReflectionType $type) => TypeModel::fromReflection(
                $type,
            ), $reflectionType->getTypes()),
            ReflectionNamedType::class => [TypeModel::fromReflection($reflectionType)],
            ReflectionUnionType::class => array_map(static fn(ReflectionType $type) => TypeModel::fromReflection(
                $type,
            ), $reflectionType->getTypes()),
            default => [],
        };

        return new self($types);
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function normalize(): array
    {
        return array_map(static fn(TypeModel $type) => $type->normalize(), $this->types);
    }

    public function toString(): string
    {
        $normalizedNames = [];

        foreach ($this->types as $type) {
            $normalizedNames[] = $type->name();
        }

        return implode('|', $normalizedNames);
    }
}

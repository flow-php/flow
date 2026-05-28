<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use ReflectionAttribute;
use ReflectionFunctionAbstract;

use function count;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;

final readonly class AttributesModel
{
    /**
     * @param array<AttributeModel> $attributes
     */
    public function __construct(
        public array $attributes,
    ) {}

    /**
     * @param array<array<string, mixed>> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_list(type_map(type_string(), type_mixed()))->assert($data);

        return new self(array_map(AttributeModel::fromArray(...), $data));
    }

    public static function fromReflection(ReflectionFunctionAbstract $reflection): self
    {
        $attributes = [];

        foreach ($reflection->getAttributes() as $attribute) {
            $attributes[] = AttributeModel::fromReflection(type_instance_of(ReflectionAttribute::class)->assert(
                $attribute,
            ));
        }

        return new self($attributes);
    }

    public function findByName(string $name): ?AttributeModel
    {
        $attributes = array_filter(
            $this->attributes,
            static fn(AttributeModel $attribute) => $attribute->name === $name,
        );

        if (count($attributes)) {
            return $attributes[0];
        }

        return null;
    }

    /**
     * @return array<array<string, mixed>>
     */
    public function normalize(): array
    {
        return array_map(static fn(AttributeModel $attribute) => $attribute->normalize(), $this->attributes);
    }
}

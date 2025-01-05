<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

final class AttributesModel
{
    public function __construct(
        public readonly array $attributes,
    ) {
    }

    public static function fromArray(array $data) : self
    {
        return new self(
            array_map(static fn (array $attribute) => AttributeModel::fromArray($attribute), $data),
        );
    }

    public static function fromReflection(\ReflectionFunction $reflection) : self
    {
        return new self(
            array_map(
                fn (\ReflectionAttribute $reflectionAttribute) : AttributeModel => AttributeModel::fromReflection($reflectionAttribute),
                $reflection->getAttributes()
            )
        );
    }

    public function normalize() : array
    {
        return array_map(fn (AttributeModel $attribute) => $attribute->normalize(), $this->attributes);
    }
}

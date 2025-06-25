<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function Flow\Types\DSL\{type_boolean, type_optional, type_string, type_structure};

final class TypeModel
{
    public function __construct(
        public readonly string $name,
        public readonly ?string $namespace,
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
            'namespace' => type_optional(type_string()),
            'is_nullable' => type_boolean(),
            'is_variadic' => type_boolean(),
        ])->assert($data);

        return new self(
            $data['name'],
            $data['namespace'],
            $data['is_nullable'],
            $data['is_variadic'],
        );
    }

    public static function fromReflection(\ReflectionType $reflectionType) : self
    {
        if (!$reflectionType instanceof \ReflectionNamedType) {
            throw new \InvalidArgumentException('ReflectionType must be instance of ReflectionNamedType');
        }

        $name = $reflectionType->getName();

        $isClass = \class_exists($name) || \interface_exists($name) || \enum_exists($name);

        return new self(
            $isClass ? (new \ReflectionClass($name))->getShortName() : $name,
            $isClass ? (new \ReflectionClass($name))->getNamespaceName() : null,
            $reflectionType->allowsNull(),
            false,
        );
    }

    public function name() : string
    {
        if (\class_exists($this->name)) {
            return (new \ReflectionClass($this->name))->getShortName();
        }

        return $this->name;
    }

    public function namespace() : ?string
    {
        if (\class_exists($this->name)) {
            return (new \ReflectionClass($this->name))->getNamespaceName();
        }

        return null;
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'namespace' => $this->namespace,
            'is_nullable' => $this->isNullable,
            'is_variadic' => $this->isVariadic,
        ];
    }
}

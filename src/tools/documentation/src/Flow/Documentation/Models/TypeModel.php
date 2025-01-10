<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

final class TypeModel
{
    public function __construct(
        public readonly string $name,
        public readonly ?string $namespace,
        public readonly bool $isNullable,
        public readonly bool $isVariadic,
    ) {
    }

    public static function fromArray(array $data) : self
    {
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

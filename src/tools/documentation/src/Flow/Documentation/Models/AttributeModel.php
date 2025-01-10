<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

final class AttributeModel
{
    public function __construct(
        public readonly string $name,
        public readonly string $namespace,
        public readonly array $arguments,
    ) {
    }

    public static function fromArray(array $data) : self
    {
        return new self(
            $data['name'],
            $data['namespace'],
            $data['arguments'],
        );
    }

    /**
     * @param \ReflectionAttribute<object> $reflectionAttribute
     */
    public static function fromReflection(\ReflectionAttribute $reflectionAttribute) : self
    {
        $attributeReflectionClass = new \ReflectionClass($reflectionAttribute->getName());

        return new self(
            $attributeReflectionClass->getShortName(),
            ($attributeReflectionClass)->getNamespaceName(),
            $reflectionAttribute->getArguments()
        );
    }

    public function normalize() : array
    {
        return [
            'name' => $this->name,
            'namespace' => $this->namespace,
            'arguments' => $this->arguments,
        ];
    }
}

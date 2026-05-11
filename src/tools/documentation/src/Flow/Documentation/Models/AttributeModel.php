<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

final readonly class AttributeModel
{
    /**
     * @param array<string, mixed> $arguments
     */
    public function __construct(
        public string $name,
        public string $namespace,
        public array $arguments,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'name' => type_string(),
            'namespace' => type_string(),
            'arguments' => type_map(type_string(), type_mixed()),
        ])->assert($data);

        return new self($data['name'], $data['namespace'], $data['arguments']);
    }

    /**
     * @param \ReflectionAttribute<object> $reflectionAttribute
     */
    public static function fromReflection(\ReflectionAttribute $reflectionAttribute): self
    {
        $attributeReflectionClass = new \ReflectionClass($reflectionAttribute->getName());

        return new self(
            $attributeReflectionClass->getShortName(),
            $attributeReflectionClass->getNamespaceName(),
            $reflectionAttribute->getArguments(),
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'namespace' => $this->namespace,
            'arguments' => $this->arguments,
        ];
    }
}

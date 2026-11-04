<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use InvalidArgumentException;
use ReflectionClass;
use Roave\BetterReflection\Reflection\ReflectionNamedType;
use Roave\BetterReflection\Reflection\ReflectionType;

use function class_exists;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function in_array;
use function strrpos;
use function substr;

final readonly class TypeModel
{
    public function __construct(
        public string $name,
        public ?string $namespace,
        public bool $isNullable,
        public bool $isVariadic,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'name' => type_string(),
            'namespace' => type_optional(type_string()),
            'is_nullable' => type_boolean(),
            'is_variadic' => type_boolean(),
        ])->assert($data);

        return new self($data['name'], $data['namespace'], $data['is_nullable'], $data['is_variadic']);
    }

    public static function fromReflection(ReflectionType $reflectionType): self
    {
        if (!$reflectionType instanceof ReflectionNamedType) {
            throw new InvalidArgumentException('ReflectionType must be instance of ReflectionNamedType');
        }

        $name = $reflectionType->getName();

        if ($reflectionType->isBuiltin() || in_array($name, ['self', 'parent'], true)) {
            return new self($name, null, $reflectionType->allowsNull(), false);
        }

        $separator = strrpos($name, '\\');

        return new self(
            $separator === false ? $name : substr($name, $separator + 1),
            $separator === false ? '' : substr($name, 0, $separator),
            $reflectionType->allowsNull(),
            false,
        );
    }

    public function name(): string
    {
        if (class_exists($this->name)) {
            return (new ReflectionClass($this->name))->getShortName();
        }

        return $this->name;
    }

    public function namespace(): ?string
    {
        if (class_exists($this->name)) {
            return (new ReflectionClass($this->name))->getNamespaceName();
        }

        return null;
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'namespace' => $this->namespace,
            'is_nullable' => $this->isNullable,
            'is_variadic' => $this->isVariadic,
        ];
    }
}

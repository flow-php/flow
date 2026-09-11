<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use InvalidArgumentException;
use ReflectionParameter;
use Throwable;

use function addslashes;
use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_object;
use function is_string;
use function str_replace;

final readonly class ParameterModel
{
    /**
     * @param string $name
     * @param TypesModel $type
     * @param bool $isNullable
     * @param bool $isVariadic
     * @param ?string $defaultValue
     */
    public function __construct(
        public string $name,
        public TypesModel $type,
        public bool $hasDefaultValue,
        public bool $isNullable,
        public bool $isVariadic,
        public ?string $defaultValue = null,
    ) {}

    /**
     * @param array<string, mixed> $data
     */
    public static function fromArray(array $data): self
    {
        $data = type_structure([
            'name' => type_string(),
            'type' => type_array(),
            'has_default_value' => type_boolean(),
            'is_nullable' => type_boolean(),
            'is_variadic' => type_boolean(),
            'default_value' => type_optional(type_string()),
        ])->assert($data);

        /** @var array<array<string, mixed>> $type */
        $type = $data['type'];

        return new self(
            $data['name'],
            TypesModel::fromArray($type),
            $data['has_default_value'],
            $data['is_nullable'],
            $data['is_variadic'],
            $data['default_value'],
        );
    }

    public static function fromReflection(ReflectionParameter $reflectionParameter): self
    {
        $defaultValue = null;
        $hasDefaultValue = false;

        try {
            $defaultValue = self::exportDefaultValue($reflectionParameter->getDefaultValue());
            $hasDefaultValue = true;
        } catch (Throwable) {
            $hasDefaultValue = false;
        }

        $reflectionType = $reflectionParameter->getType();

        if ($reflectionType === null) {
            throw new InvalidArgumentException('ReflectionType must be instance of ReflectionNamedType');
        }

        return new self(
            $reflectionParameter->getName(),
            TypesModel::fromReflection($reflectionType),
            $hasDefaultValue,
            $reflectionParameter->allowsNull(),
            $reflectionParameter->isVariadic(),
            $defaultValue,
        );
    }

    /**
     * @return array<string, mixed>
     */
    public function normalize(): array
    {
        return [
            'name' => $this->name,
            'type' => $this->type->normalize(),
            'has_default_value' => $this->hasDefaultValue,
            'is_nullable' => $this->isNullable,
            'is_variadic' => $this->isVariadic,
            'default_value' => $this->defaultValue,
        ];
    }

    private static function exportDefaultValue(mixed $value): string
    {
        if ($value === null) {
            return 'null';
        }

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_string($value)) {
            $escaped = addslashes($value);
            $escaped = str_replace(["\n", "\r", "\t"], ['\\n', '\\r', '\\t'], $escaped);

            return "'" . $escaped . "'";
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        if (is_array($value)) {
            if (empty($value)) {
                return '[]';
            }

            return '[...]';
        }

        if (is_object($value)) {
            return $value::class . '::...';
        }

        return '...';
    }
}

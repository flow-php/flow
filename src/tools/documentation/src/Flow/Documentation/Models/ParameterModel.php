<?php

declare(strict_types=1);

namespace Flow\Documentation\Models;

use PhpParser\Node\Expr;
use PhpParser\Node\Expr\ConstFetch;
use PhpParser\PrettyPrinter\Standard;
use Roave\BetterReflection\Reflection\ReflectionParameter;

use function Flow\Types\DSL\type_array;
use function Flow\Types\DSL\type_boolean;
use function Flow\Types\DSL\type_instance_of;
use function Flow\Types\DSL\type_optional;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;

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
        $hasDefaultValue = $reflectionParameter->isDefaultValueAvailable();
        $reflectionType = $reflectionParameter->getType();

        return new self(
            $reflectionParameter->getName(),
            $reflectionType !== null ? TypesModel::fromReflection($reflectionType) : new TypesModel([]),
            $hasDefaultValue,
            $reflectionParameter->allowsNull(),
            $reflectionParameter->isVariadic(),
            $hasDefaultValue
                ? self::exportDefaultValue(
                    type_instance_of(Expr::class)->assert($reflectionParameter->getDefaultValueExpression()),
                )
                : null,
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

    private static function exportDefaultValue(Expr $defaultValue): string
    {
        // PHP stubs write constants fully qualified (\null, \PHP_INT_MAX), source code does not.
        return $defaultValue instanceof ConstFetch
            ? $defaultValue->name->toString()
            : (new Standard())->prettyPrintExpr($defaultValue);
    }
}

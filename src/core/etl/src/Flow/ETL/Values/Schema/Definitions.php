<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Schema;

use Flow\ETL\Foundation\Exception\RuntimeException;
use Flow\ETL\Foundation\Exception\UnsupportedUnionTypeException;
use Flow\ETL\Values\Schema\Definition\BooleanDefinition;
use Flow\ETL\Values\Schema\Definition\DateDefinition;
use Flow\ETL\Values\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Values\Schema\Definition\EnumDefinition;
use Flow\ETL\Values\Schema\Definition\FloatDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLDefinition;
use Flow\ETL\Values\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Values\Schema\Definition\IntegerDefinition;
use Flow\ETL\Values\Schema\Definition\JsonDefinition;
use Flow\ETL\Values\Schema\Definition\ListDefinition;
use Flow\ETL\Values\Schema\Definition\MapDefinition;
use Flow\ETL\Values\Schema\Definition\NullDefinition;
use Flow\ETL\Values\Schema\Definition\StringDefinition;
use Flow\ETL\Values\Schema\Definition\StructureDefinition;
use Flow\ETL\Values\Schema\Definition\TimeDefinition;
use Flow\ETL\Values\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Values\Schema\Definition\UuidDefinition;
use Flow\ETL\Values\Schema\Definition\XMLDefinition;
use Flow\ETL\Values\Schema\Definition\XMLElementDefinition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\StructureType;
use Flow\Types\Type\Logical\TimeType;
use Flow\Types\Type\Logical\TimeZoneType;
use Flow\Types\Type\Logical\UuidType;
use Flow\Types\Type\Logical\XMLElementType;
use Flow\Types\Type\Logical\XMLType;
use Flow\Types\Type\Native\ArrayType;
use Flow\Types\Type\Native\BooleanType;
use Flow\Types\Type\Native\EmptyArrayType;
use Flow\Types\Type\Native\EnumType;
use Flow\Types\Type\Native\FloatType;
use Flow\Types\Type\Native\IntegerType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use Flow\Types\Type\Native\UnionType;
use Flow\Types\Type\TypeFactory;
use Flow\Types\Type\TypeWidener;
use UnitEnum;

use function array_key_exists;
use function Flow\Types\DSL\type_null;
use function is_array;
use function is_bool;
use function is_string;
use function sprintf;

final readonly class Definitions
{
    /**
     * The definition normalize() wrote: its name ("ref"), type, nullability and metadata.
     *
     * @param array<array-key, mixed> $definition
     *
     * @throws RuntimeException
     *
     * @return Definition<mixed>
     */
    public static function fromArray(array $definition): Definition
    {
        if (!array_key_exists('ref', $definition) || !is_string($definition['ref'])) {
            throw new RuntimeException('Definition array must have a string "ref" key');
        }

        if (!array_key_exists('type', $definition) || !is_array($definition['type'])) {
            throw new RuntimeException('Definition array must have an array "type" key');
        }

        /** @var array<string, mixed> $typeData */
        $typeData = $definition['type'];

        /** @var array<string, array<mixed>|bool|float|int|string> $metadataData */
        $metadataData = array_key_exists('metadata', $definition) && is_array($definition['metadata'])
            ? $definition['metadata']
            : [];

        return self::of(
            $definition['ref'],
            TypeFactory::fromArray($typeData),
            array_key_exists('nullable', $definition) && is_bool($definition['nullable']) && $definition['nullable'],
            Metadata::fromArray($metadataData),
        );
    }

    /**
     * $left and $right, one column described twice, widened to the definition both fit: the type TypeWidener widens
     * them to, nullable when either is, and both metadata merged.
     *
     * @param Definition<mixed> $left
     * @param Definition<mixed> $right
     *
     * @throws RuntimeException the two describe different columns
     *
     * @return Definition<mixed>
     */
    public static function merged(Definition $left, Definition $right): Definition
    {
        if ($left->name() !== $right->name()) {
            throw new RuntimeException(sprintf(
                'Cannot merge different definitions, %s and %s',
                $left->name(),
                $right->name(),
            ));
        }

        $leftType = $left->type();
        $rightType = $right->type();

        // a null enum value carries no class, so it is declared as the UnitEnum base: the concrete side wins
        if ($leftType instanceof EnumType && $rightType instanceof EnumType) {
            [$leftType, $rightType] = [
                $leftType->class === UnitEnum::class ? $rightType : $leftType,
                $rightType->class === UnitEnum::class ? $leftType : $rightType,
            ];
        }

        $widener = new TypeWidener();

        return self::of(
            $left->name(),
            $widener->widen(
                $left->isNullable() ? $widener->nullable($leftType) : $leftType,
                $right->isNullable() ? $widener->nullable($rightType) : $rightType,
            ),
            metadata: $left->metadata()->merge($right->metadata()),
        );
    }

    /**
     * The definition a column of $type has: one of the 19 Flow definitions, or a refusal.
     *
     * @param Type<mixed> $type
     *
     * @return Definition<mixed>
     */
    public static function of(string $name, Type $type, bool $nullable = false, ?Metadata $metadata = null): Definition
    {
        return match (true) {
            $type instanceof BooleanType => new BooleanDefinition($name, $nullable, $metadata),
            $type instanceof IntegerType => new IntegerDefinition($name, $nullable, $metadata),
            $type instanceof FloatType => new FloatDefinition($name, $nullable, $metadata),
            $type instanceof StringType => new StringDefinition($name, $nullable, $metadata),
            $type instanceof NumericStringType => new StringDefinition($name, $nullable, $metadata, $type),
            $type instanceof DateType => new DateDefinition($name, $nullable, $metadata),
            $type instanceof DateTimeType => new DateTimeDefinition($name, $type, $nullable, $metadata),
            $type instanceof TimeType => new TimeDefinition($name, $nullable, $metadata),
            $type instanceof JsonType => new JsonDefinition($name, $nullable, $metadata),
            $type instanceof ArrayType => new JsonDefinition($name, $nullable, $metadata),
            // @mago-expect linter:no-fully-qualified-global-function
            $type instanceof EmptyArrayType => throw new RuntimeException(\sprintf(
                'Column "%s" cannot be typed as array{} - an empty array is a value, not a column type. '
                . 'Declare the element type, e.g. type_list(type_string()).',
                $name,
            )),
            $type instanceof UuidType => new UuidDefinition($name, $nullable, $metadata),
            $type instanceof TimeZoneType => new TimeZoneDefinition($name, $nullable, $metadata),
            $type instanceof ListType => new ListDefinition($name, $type, $nullable, $metadata),
            $type instanceof MapType => new MapDefinition($name, $type, $nullable, $metadata),
            $type instanceof StructureType => new StructureDefinition($name, $type, $nullable, $metadata),
            // Iceberg's rule: ["null", T] is the one legal union and it means ?T, not a sum type.
            $type instanceof UnionType => $type->isOptionalType()
                ? self::of(
                    $name,
                    $type->types()->without(type_null())->first() ?? throw UnsupportedUnionTypeException::forColumn(
                        $name,
                        $type,
                    ),
                    nullable: true,
                    metadata: $metadata,
                )
                : throw UnsupportedUnionTypeException::forColumn($name, $type),
            $type instanceof EnumType => new EnumDefinition($name, $type->class, $nullable, $metadata),
            $type instanceof HTMLType => new HTMLDefinition($name, $nullable, $metadata),
            $type instanceof HTMLElementType => new HTMLElementDefinition($name, $nullable, $metadata),
            $type instanceof XMLType => new XMLDefinition($name, $nullable, $metadata),
            $type instanceof XMLElementType => new XMLElementDefinition($name, $nullable, $metadata),
            $type instanceof NullType => new NullDefinition($name, $metadata),
            $type instanceof OptionalType => self::of($name, $type->base(), nullable: true, metadata: $metadata),
            // @mago-expect linter:no-fully-qualified-global-function
            default => throw new RuntimeException(\sprintf('Cannot create Definition from type: %s', $type::class)),
        };
    }
}

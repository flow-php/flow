<?php

declare(strict_types=1);

namespace Flow\ETL\Column\Physical;

use BackedEnum;
use Flow\ETL\Exception\ColumnMismatchException;
use Flow\ETL\Exception\InvalidArgumentException;
use Flow\ETL\Schema\Definition;
use Flow\ETL\Schema\Definition\BooleanDefinition;
use Flow\ETL\Schema\Definition\DateDefinition;
use Flow\ETL\Schema\Definition\DateTimeDefinition;
use Flow\ETL\Schema\Definition\EnumDefinition;
use Flow\ETL\Schema\Definition\FloatDefinition;
use Flow\ETL\Schema\Definition\HTMLDefinition;
use Flow\ETL\Schema\Definition\HTMLElementDefinition;
use Flow\ETL\Schema\Definition\IntegerDefinition;
use Flow\ETL\Schema\Definition\JsonDefinition;
use Flow\ETL\Schema\Definition\ListDefinition;
use Flow\ETL\Schema\Definition\MapDefinition;
use Flow\ETL\Schema\Definition\NullDefinition;
use Flow\ETL\Schema\Definition\StringDefinition;
use Flow\ETL\Schema\Definition\StructureDefinition;
use Flow\ETL\Schema\Definition\TimeDefinition;
use Flow\ETL\Schema\Definition\TimeZoneDefinition;
use Flow\ETL\Schema\Definition\UuidDefinition;
use Flow\ETL\Schema\Definition\XMLDefinition;
use Flow\ETL\Schema\Definition\XMLElementDefinition;
use Flow\Types\Type;
use Flow\Types\Type\Logical\ClassStringType;
use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;
use Flow\Types\Type\Logical\HTMLElementType;
use Flow\Types\Type\Logical\HTMLType;
use Flow\Types\Type\Logical\JsonType;
use Flow\Types\Type\Logical\ListType;
use Flow\Types\Type\Logical\MapType;
use Flow\Types\Type\Logical\NonEmptyStringType;
use Flow\Types\Type\Logical\NumericStringType;
use Flow\Types\Type\Logical\OptionalType;
use Flow\Types\Type\Logical\PositiveIntegerType;
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
use Flow\Types\Type\Native\MixedType;
use Flow\Types\Type\Native\NullType;
use Flow\Types\Type\Native\StringType;
use UnitEnum;

use function Flow\Types\DSL\type_bare;
use function in_array;
use function sprintf;

final readonly class PhysicalFor
{
    private const array DEFINITIONS = [
        BooleanDefinition::class,
        DateDefinition::class,
        DateTimeDefinition::class,
        EnumDefinition::class,
        FloatDefinition::class,
        HTMLDefinition::class,
        HTMLElementDefinition::class,
        IntegerDefinition::class,
        JsonDefinition::class,
        ListDefinition::class,
        MapDefinition::class,
        NullDefinition::class,
        StringDefinition::class,
        StructureDefinition::class,
        TimeDefinition::class,
        TimeZoneDefinition::class,
        UuidDefinition::class,
        XMLDefinition::class,
        XMLElementDefinition::class,
    ];

    /**
     * @param Definition<mixed> $definition
     *
     * @throws ColumnMismatchException
     */
    public function definition(Definition $definition): Physical
    {
        if (!in_array($definition::class, self::DEFINITIONS, true)) {
            throw ColumnMismatchException::unsupportedType(
                $definition,
                'only the 19 Flow definitions have a column kind',
            );
        }

        try {
            return $this->type($definition->type());
        } catch (InvalidArgumentException $e) {
            throw ColumnMismatchException::unsupportedType($definition, $e->getMessage());
        }
    }

    /**
     * A column of $type exists: definition_from_type() accepts it and definition() gives it a Physical.
     *
     * @param Type<mixed> $type
     */
    public function supports(Type $type): bool
    {
        $base = type_bare($type);

        return match (true) {
            $base instanceof ArrayType => true,
            $base instanceof EmptyArrayType,
            $base instanceof PositiveIntegerType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
                => false,
            default => $this->supportsChild($base),
        };
    }

    /**
     * A child (list element, map value, structure element) of $type exists once TypeProjection stored it as json
     * (an array) or as optional (a nullable union): type() gives it a Physical.
     *
     * @param Type<mixed> $type
     */
    public function supportsChild(Type $type): bool
    {
        $base = type_bare($type);

        return match (true) {
            $base instanceof ArrayType, $base instanceof EmptyArrayType => true,
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof FloatType,
            $base instanceof BooleanType,
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
            $base instanceof DateTimeType,
            $base instanceof DateType,
            $base instanceof TimeType,
            $base instanceof UuidType,
            $base instanceof TimeZoneType,
            $base instanceof JsonType,
            $base instanceof XMLType,
            $base instanceof XMLElementType,
            $base instanceof HTMLType,
            $base instanceof HTMLElementType,
            $base instanceof NullType,
                => true,
            $base instanceof EnumType => $base->class !== UnitEnum::class && $base->class !== BackedEnum::class,
            $base instanceof ListType => $this->supportsChild($base->element()),
            $base instanceof MapType => $this->supportsChild($base->value()),
            $base instanceof StructureType => $this->supportsElements($base),
            default => false,
        };
    }

    /**
     * @param StructureType<mixed> $type
     */
    public function supportsElements(StructureType $type): bool
    {
        foreach ($type->elements() as $element) {
            if (!$this->supportsChild($element->type)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param Type<mixed> $type
     *
     * @throws InvalidArgumentException
     */
    public function type(Type $type): Physical
    {
        $base = $type instanceof OptionalType ? $type->base() : $type;

        return match (true) {
            $base instanceof IntegerType,
            $base instanceof PositiveIntegerType,
            $base instanceof FloatType,
            $base instanceof BooleanType,
            $base instanceof StringType,
            $base instanceof NonEmptyStringType,
            $base instanceof NumericStringType,
            $base instanceof ClassStringType,
                => new IdentityPhysical(),
            $base instanceof DateTimeType => new DateTimePhysical($base->zone()),
            $base instanceof DateType => new DatePhysical(),
            $base instanceof TimeType => new TimePhysical(),
            $base instanceof UuidType => new UuidPhysical(),
            $base instanceof TimeZoneType => new TimeZonePhysical(),
            $base instanceof JsonType => new JsonPhysical(),
            $base instanceof EnumType => $base->class === UnitEnum::class || $base->class === BackedEnum::class
                ? throw new InvalidArgumentException('declare the concrete enum class')
                : new EnumPhysical($base->class),
            $base instanceof XMLType => new XmlDocumentPhysical(),
            $base instanceof XMLElementType => new XmlElementPhysical(),
            $base instanceof HTMLType => new HtmlDocumentPhysical(),
            $base instanceof HTMLElementType => new HtmlElementPhysical(),
            $base instanceof ListType => ($element = $this->type($base->element())) instanceof IdentityPhysical
                ? $element
                : new ListPhysical($element),
            $base instanceof MapType => ($value = $this->type($base->value())) instanceof IdentityPhysical
                ? $value
                : new MapPhysical($value),
            $base instanceof StructureType => array_filter(
                $elements = $this->elements($base),
                static fn(Physical $element): bool => !$element instanceof IdentityPhysical,
            ) === []
                    ? new IdentityPhysical()
                    : new StructPhysical($elements),
            $base instanceof NullType => new NullPhysical(),
            $base instanceof MixedType => throw new InvalidArgumentException(
                'a mixed element has no column kind, declare it or use json',
            ),
            default => throw new InvalidArgumentException(sprintf('type %s has no column kind', $base->toString())),
        };
    }

    /**
     * @param StructureType<mixed> $type
     *
     * @return array<array-key, Physical>
     */
    public function elements(StructureType $type): array
    {
        $elements = [];

        foreach ($type->elements() as $element) {
            $elements[$element->name] = $this->type($element->type);
        }

        return $elements;
    }
}
